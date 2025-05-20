import os
import importlib.util
import yaml

def load_and_run_agent(config_path: str):
    try:
        print(f"🧪 [Manager] 에이전트 로드 시작: {config_path}", flush=True)

        # agent.py 경로 지정
        agent_dir = os.path.dirname(config_path)
        agent_module_path = os.path.join(agent_dir, "agent.py")

        # 동적 로드
        spec = importlib.util.spec_from_file_location("agent_module", agent_module_path)
        agent_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(agent_module)

        # Agent 클래스 로딩 및 실행
        AgentClass = getattr(agent_module, "Agent")
        agent = AgentClass(config_path)
        agent.run()

    except Exception as e:
        print(f"❌ [Manager] 에이전트 실행 실패: {config_path} | 오류: {e}", flush=True)

def get_all_agent_configs(base_path="agents_basket"):
    config_files = []
    for dir_name in os.listdir(base_path):
        for ext in ["config.yaml", "config.yml"]:
            config_path = os.path.join(base_path, dir_name, ext)
            if os.path.isfile(config_path):
                config_files.append(config_path)
                break  # 하나만 찾으면 됨
    return config_files