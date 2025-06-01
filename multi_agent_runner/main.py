import os
import sys
import yaml
import subprocess
import multiprocessing

CONFIG_PATH = "agents_basket/common/central_config.yml"
BASE_MODULE_PATH = "agents_basket"

def validate_config(path: str):
    if not os.path.exists(path):
        raise FileNotFoundError(f"설정 파일이 존재하지 않습니다: {path}")
    with open(path) as f:
        yaml.safe_load(f)
    print(f"✅ 설정 파일 로딩 성공: {path}", flush=True)

def get_agent_runs(central_config_path: str):
    with open(central_config_path, "r") as f:
        config = yaml.safe_load(f)
    return [(name, central_config_path) for name in config.keys()]

def run_agent(agent_key: str, config_path: str):
    module_path = f"{BASE_MODULE_PATH}.{agent_key}.agent"

    # 💡 모든 agent에 config_key까지 전달
    cmd = [sys.executable, "-m", module_path, config_path, agent_key]

    print(f"🧪 [Runner] {agent_key} 실행 시작... (module_path: {module_path} | 인자: {cmd})", flush=True)
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        print(result.stdout)
        print(result.stderr, file=sys.stderr)
        if result.returncode != 0:
            print(f"❌ [Runner] {agent_key} 실패 (코드 {result.returncode})", flush=True)
    except Exception as e:
        print(f"❌ [Runner] {agent_key} 실행 중 예외 발생: {e}", flush=True)

def main():
    try:
        validate_config(CONFIG_PATH)
    except Exception as e:
        print(f"❌ config 검증 실패: {e}", flush=True)
        sys.exit(1)

    agent_runs = get_agent_runs(CONFIG_PATH)
    print(f"🚀 실행할 에이전트 개수: {len(agent_runs)}", flush=True)

    processes = []
    for agent_key, config_path in agent_runs:
        p = multiprocessing.Process(target=run_agent, args=(agent_key, config_path))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

if __name__ == "__main__":
    main()
