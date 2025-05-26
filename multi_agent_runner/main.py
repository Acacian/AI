import sys
import multiprocessing
from agent_manager import get_all_agent_configs, load_and_run_agent

# 🔹 config 검증 및 정렬
from agents_basket.common.validate_config import validate_and_sort_config

CONFIG_PATH = "collector/config.yml"

def main():
    # ✅ 실행 전 config 검증
    try:
        validate_and_sort_config(CONFIG_PATH)
    except Exception as e:
        print(f"❌ config 검증 실패: {e}", flush=True)
        sys.exit(1)

    # 🔹 에이전트 실행
    config_paths = get_all_agent_configs()
    print(f"🚀 실행할 에이전트 개수: {len(config_paths)}", flush=True)

    processes = []
    for config_path in config_paths:
        p = multiprocessing.Process(
            target=load_and_run_agent,
            args=(config_path,),
            daemon=True
        )
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

if __name__ == "__main__":
    main()
