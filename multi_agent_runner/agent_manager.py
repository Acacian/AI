import os, subprocess, sys

def load_and_run_agent(config_path: str):
    try:
        print(f"🧪 [Manager] 에이전트 로드 시작: {config_path}", flush=True)

        agent_dir = os.path.dirname(config_path)
        agent_module_path = os.path.join(agent_dir, "agent.py")

        # 모듈 경로 → "agents_basket.pattern_ae.agent" 형식으로 변환
        base_dir = "agents_basket"
        relative_module = agent_dir.replace("/", ".").replace("\\", ".")
        if relative_module.startswith(base_dir + "."):
            module_path = f"{relative_module}.agent"
        else:
            raise ValueError(f"잘못된 에이전트 경로: {agent_dir}")

        subprocess.run(
            [sys.executable, "-m", module_path, config_path],
            check=True,
            stdout=sys.stdout,
            stderr=sys.stderr
        )

    except subprocess.CalledProcessError as e:
        print(f"❌ [Manager] 에이전트 실행 실패: {config_path} | 오류: {e}", flush=True)
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