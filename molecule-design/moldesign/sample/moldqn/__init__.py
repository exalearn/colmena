import logging
import networkx as nx
from typing import Set, List, Callable

from rdkit import RDLogger
from sklearn.base import BaseEstimator
from molgym.agents.preprocessing import MorganFingerprints
from molgym.agents.moldqn import DQNFinalState
from molgym.envs.simple import Molecule
from molgym.envs.rewards import RewardFunction
from molgym.utils.conversions import convert_nx_to_smiles

# Set up the logger
logger = logging.getLogger(__name__)
rdkit_logger = RDLogger.logger()
rdkit_logger.setLevel(RDLogger.CRITICAL)


class _SklearnReward(RewardFunction):
    def __init__(self, function: Callable[[List[str]], List[float]],
                 maximize: bool = True):
        super().__init__(maximize)
        self.function = function

    def _call(self, graph: nx.Graph) -> float:
        if graph is None:
            return 0
        return self.function([convert_nx_to_smiles(graph)])[0]


def generate_molecules(target_fn: BaseEstimator, episodes: int = 10,
                       n_steps: int = 32, update_q_every: int = 10) -> Set[str]:
    """Perform the RL experiment

    Args:
        target_fn (Callable): Function to be optimized. Takes a list of entries as input
            and returns a list of values as the target. Higher values of the function are better
        episodes (int): Number of episodes to run
        n_steps (int): Maximum number of steps per episode
        update_q_every (int): After how many updates to update the Q function
    Returns:
        ([str]) List of molecules that were created
    """

    # Make the environment
    #  We do not yet support vectorized target functions, so use this wrapper to
    reward = _SklearnReward(target_fn.predict)
    env = Molecule(reward=reward)

    # Run the reinforcement learning
    best_reward = 0

    # Prepare the output
    output = set()

    # Make the agent
    agent = DQNFinalState(env, MorganFingerprints(), epsilon=1.0, epsilon_decay=0.9995)

    # Keep track of the smiles strings
    for e in range(episodes):
        current_state = env.reset()
        logger.info(f'Starting episode {e+1}/{episodes}')
        for s in range(n_steps):
            # Get action based on current state
            action, _, _ = agent.action()

            # Fix cluster action
            new_state, reward, done, _ = env.step(action)

            # Check if it's the last step and flag as done
            if s == n_steps:
                logger.debug('Last step  ... done')
                done = True

            # Add the state to the output
            output.add(env.state)

            # Save outcome
            agent.remember(current_state, action, reward,
                           new_state, agent.env.action_space.get_possible_actions(), done)

            # Train model
            agent.train()

            # Update state
            current_state = new_state

            if best_reward > reward:
                best_reward = reward
                logger.info("Best reward: %s" % best_reward)

            if done:
                break

        # Update the Q network after certain numbers of episodes and adjust epsilon
        if e > 0 and e % update_q_every == 0:
            agent.update_target_q_network()
        agent.epsilon_adj()

    # Convert the outputs back to SMILES strings
    output = set(convert_nx_to_smiles(x) for x in output)
    return output
