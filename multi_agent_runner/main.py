from agent_manager import get_all_agent_configs, load_and_run_agent
import multiprocessing

def main():
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
