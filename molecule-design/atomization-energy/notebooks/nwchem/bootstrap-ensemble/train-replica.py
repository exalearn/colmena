from argparse import ArgumentParser
import json
import os

from tensorflow.keras.optimizers import Adam
from tensorflow.keras.models import load_model
from tensorflow.keras import callbacks as cb
import tensorflow as tf
from scipy.stats import spearmanr, kendalltau
import pandas as pd
import numpy as np

from molgym.mpnn.callbacks import EpochTimeLogger, LRLogger
from molgym.mpnn.layers import custom_objects
from molgym.mpnn.data import make_data_loader

model_dir = os.path.join('..', '..', '..', '..', 'notebooks', 'mpnn-atomization-energy')


if __name__ == "__main__":
    # Define the command line arguments
    arg_parser = ArgumentParser()
    arg_parser.add_argument('--batch-size', help='Number of molecules per batch', type=int, default=32)
    arg_parser.add_argument('--num-epochs', help='Number of epochs to run', type=int, default=512)
    arg_parser.add_argument('random_seed', help='Random seed for data bootstrap sample', type=int)

    # Parse the arguments
    args = arg_parser.parse_args()
    run_params = args.__dict__

    # Determine the output directory
    test_dir = os.path.join('networks', f'b{args.batch_size}_n{args.num_epochs}_S{args.random_seed}')
    os.makedirs(test_dir, exist_ok=True)
    with open(os.path.join(test_dir, 'config.json'), 'w') as fp:
        json.dump(run_params, fp)

    # Load in the training database and downsample it
    train_data = list(iter(tf.data.TFRecordDataset(os.path.join('..', 'train_data.proto'))))
    rng = np.random.RandomState(args.random_seed)
    train_data = rng.choice(train_data, size=(len(train_data),), replace=True)

    # Save it back to disk for later user
    train_path = os.path.join(test_dir, 'train_data.proto')
    with tf.io.TFRecordWriter(train_path) as writer:
        for d in train_data:
            writer.write(d.numpy())

    # Making the data loaders for use during training
    train_loader = make_data_loader(train_path, shuffle_buffer=len(train_data), cache=True, output_property='u0_atom',
                                    batch_size=args.batch_size)
    test_loader = make_data_loader(os.path.join('..', 'test_data.proto'),
                                   batch_size=args.batch_size, output_property='u0_atom')
    val_loader = make_data_loader(os.path.join('..', 'valid_data.proto'),
                                  batch_size=args.batch_size, output_property='u0_atom')

    # Load in the bond and atom type information
    with open(os.path.join('..', 'atom_types.json')) as fp:
        atom_type_count = len(json.load(fp))
    with open(os.path.join('..', 'bond_types.json')) as fp:
        bond_type_count = len(json.load(fp))

    # Make the model
    model = load_model(os.path.join(model_dir, 'best_model.h5'), custom_objects=custom_objects)

    # Set the scale for the output parameter
    ic50s = np.concatenate([x[1].numpy() for x in iter(train_loader)], axis=0)
    model.get_layer('scale').set_weights([np.array([[ic50s.std()]]), np.array([ic50s.mean()])])

    # Train the model
    final_learn_rate = 1e-6
    init_learn_rate = 1e-3
    decay_rate = (final_learn_rate / init_learn_rate) ** (1. / (args.num_epochs - 1))

    def lr_schedule(epoch, lr):
        return lr * decay_rate
    model.compile(Adam(init_learn_rate), 'mean_squared_error', metrics=['mean_absolute_error'])
    history = model.fit(
        train_loader, validation_data=val_loader, epochs=args.num_epochs, verbose=True,
        shuffle=False, callbacks=[
            LRLogger(),
            EpochTimeLogger(),
            cb.LearningRateScheduler(lr_schedule),
            cb.ModelCheckpoint(os.path.join(test_dir, 'best_model.h5'), save_best_only=True),
            cb.EarlyStopping(patience=128, restore_best_weights=True),
            cb.CSVLogger(os.path.join(test_dir, 'train_log.csv')),
            cb.TerminateOnNaN()
        ]
    )

    # Run on the validation set and assess statistics
    y_true = np.hstack([x[1].numpy()[:, 0] for x in iter(test_loader)])
    y_pred = np.squeeze(model.predict(test_loader))

    pd.DataFrame({'true': y_true, 'pred': y_pred}).to_csv(os.path.join(test_dir, 'test_results.csv'), index=False)

    with open(os.path.join(test_dir, 'test_summary.json'), 'w') as fp:
        json.dump({
            'r2_score': float(np.corrcoef(y_true, y_pred)[1, 0] ** 2),  # float() converts from np.float32
            'spearmanr': float(spearmanr(y_true, y_pred)[0]),
            'kendall_tau': float(kendalltau(y_true, y_pred)[0]),
            'mae': float(np.mean(np.abs(y_pred - y_true))),
            'rmse': float(np.sqrt(np.mean(np.square(y_pred - y_true))))
        }, fp, indent=2)
