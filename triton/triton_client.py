import tritonclient.http as httpclient
import numpy as np
import re
import threading

class TritonClient:
    _lock = threading.Lock()  

    def __init__(self, url: str = "triton:8000", routing: dict = None, topics: dict = None):
        with self._lock:
            self.client = httpclient.InferenceServerClient(url=url)
        self.routing = routing or {}
        self.topics = topics or {}

    def preprocess(self, data: dict) -> np.ndarray:
        features = data.get("input")
        if not isinstance(features, list):
            raise ValueError("❌ Triton 입력 형식이 올바르지 않습니다.")
        x = np.array(features, dtype=np.float32)
        if x.ndim == 3 and x.shape[1] > 100:
            x = x[:, -100:, :]
        elif x.ndim == 2 and x.shape[0] > 100:
            x = x[-100:, :]
            x = np.expand_dims(x, axis=0)
        if x.ndim == 2:
            x = np.expand_dims(x, axis=0)
        return x

    def postprocess(self, output: np.ndarray) -> list:
        return output.tolist()

    def summarize_output(self, output: list) -> dict:
        arr = np.array(output)
        return {
            "mean": float(np.mean(arr)),
            "max": float(np.max(arr)),
            "min": float(np.min(arr)),
            "std": float(np.std(arr))
        }

    def resolve_template(self, template: str, symbol: str) -> str:
        return template.replace("{symbol}", symbol)

    def resolve_routing(self, current_topic: str) -> str | None:
        for pattern, target in self.routing.items():
            regex = pattern.replace("{symbol}", r"(?P<symbol>[a-z0-9_]+)")
            match = re.fullmatch(regex, current_topic)
            if match:
                symbol = match.group("symbol")
                resolved = target.replace("{symbol}", symbol)
                return resolved
        return None

    def get_model_name(self, current_topic: str) -> str | None:
        for pattern, agent in self.topics.items():
            regex = pattern.replace("{symbol}", r"(?P<symbol>[a-z0-9_]+)")
            match = re.fullmatch(regex, current_topic)
            if match:
                return agent
        return None

    def infer_with_routing(self, current_topic: str, data: dict) -> tuple[str, dict]:
        model_name = self.get_model_name(current_topic)
        if not model_name:
            raise ValueError(f"❌ 토픽에 해당하는 모델 없음: {current_topic}")

        with self._lock:
            x = self.preprocess(data)
            inputs = [httpclient.InferInput("INPUT", x.shape, "FP32")]
            inputs[0].set_data_from_numpy(x)
            outputs = [httpclient.InferRequestedOutput("OUTPUT")]
            result = self.client.infer(model_name, inputs=inputs, outputs=outputs)
            output = self.postprocess(result.as_numpy("OUTPUT"))
            summary = self.summarize_output(output)

        next_topic = self.resolve_routing(current_topic)
        return next_topic, summary

    def infer(self, model_name: str, data: dict) -> list:
        with self._lock:
            x = self.preprocess(data)
            inputs = [httpclient.InferInput("INPUT", x.shape, "FP32")]
            inputs[0].set_data_from_numpy(x)
            outputs = [httpclient.InferRequestedOutput("OUTPUT")]
            result = self.client.infer(model_name, inputs=inputs, outputs=outputs)
            return self.postprocess(result.as_numpy("OUTPUT"))
