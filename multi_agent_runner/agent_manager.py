import os
import glob
import importlib.util
import multiprocessing

def load_and_run_agent(config_path: str):
    agent_dir = os.path.dirname(config_path)
    agent_module_path = os.path.join(agent_dir, "agent.py")

    spec = importlib.util.spec_from_file_location("agent_module", agent_module_path)
    agent_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(agent_module)

    AgentClass = getattr(agent_module, "Agent")
    agent = AgentClass(config_path)
    agent.run()

def get_all_agent_configs(base_path="agents_1m"):
    return glob.glob(f"{base_path}/*/config.yaml")
