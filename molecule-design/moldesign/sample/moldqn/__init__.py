import logging
import networkx as nx
from typing import Set, List, Callable, Tuple

from rdkit import RDLogger
from molgym.agents.moldqn import DQNFinalState
from molgym.envs.rewards import RewardFunction
from molgym.utils.conversions import convert_nx_to_smiles

# Set up the logger
from sklearn.base import BaseEstimator

logger = logging.getLogger(__name__)
rdkit_logger = RDLogger.logger()
rdkit_logger.setLevel(RDLogger.CRITICAL)


class SklearnReward(RewardFunction):
    """Reward function that calls a scikit-learn model"""

    def __init__(self, model: BaseEstimator, maximize: bool = True):
        """
        Args:
            model: A scikit-learn model
            maximize: Whether to maximize this function
        """
        super().__init__(maximize)
        self.model = model

    def _call(self, graph: nx.Graph) -> float:
        if graph is None:
            return 0
        return self.model.predict([convert_nx_to_smiles(graph)])[0]


def generate_molecules(agent: DQNFinalState, episodes: int = 10,
                       n_steps: int = 32, update_q_every: int = 10) -> Tuple[Set[str], DQNFinalState]:
    """Perform the RL experiment

    Args:
        agent (DQNFinalState): Molecular design agent
        episodes (int): Number of episodes to run
        n_steps (int): Maximum number of steps per episode
        update_q_every (int): After how many updates to update the Q function
    Returns:
        ([str]) List of molecules that were created
    """

    # Prepare the output
    output = set()

    # Keep track of the smiles strings
    for e in range(episodes):
        current_state = agent.env.reset()
        logger.info(f'Starting episode {e+1}/{episodes}')
        for s in range(n_steps):
            # Get action based on current state
            action, _, _ = agent.action()

            # Fix cluster action
            new_state, reward, done, _ = agent.env.step(action)

            # Check if it's the last step and flag as done
            if s == n_steps:
                logger.debug('Last step  ... done')
                done = True

            # Add the state to the output
            output.add(agent.env.state)

            # Save outcome
            agent.remember(current_state, action, reward,
                           new_state, agent.env.action_space.get_possible_actions(), done)

            # Train model
            agent.train()

            # Update state
            current_state = new_state

            if done:
                break

        # Update the Q network after certain numbers of episodes and adjust epsilon
        if e > 0 and e % update_q_every == 0:
            agent.update_target_q_network()
        agent.epsilon_adj()

    # Clear out the memory: Too large to send back to client
    agent.memory.clear()

    # Convert the outputs back to SMILES strings
    output = set(convert_nx_to_smiles(x) for x in output)
    return output, agent

