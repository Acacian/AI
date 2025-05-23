import tritonclient.http as httpclient
import numpy as np

class TritonClient:
    def __init__(self, url: str = "triton:8000"):
        self.client = httpclient.InferenceServerClient(url=url)

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

    def infer(self, model_name: str, data: dict) -> list:
        x = self.preprocess(data)
        inputs = [httpclient.InferInput("INPUT", x.shape, "FP32")]
        inputs[0].set_data_from_numpy(x)
        outputs = [httpclient.InferRequestedOutput("OUTPUT")]
        result = self.client.infer(model_name, inputs=inputs, outputs=outputs)
        return self.postprocess(result.as_numpy("OUTPUT"))

    def infer_signal(self, data: dict, models: list[str]) -> dict:
        signal = {}
        for model_name in models:
            try:
                output = self.infer(model_name, data)
                signal[model_name] = self.summarize_output(output)
            except Exception as e:
                print(f"❌ 모델 '{model_name}' 추론 실패: {e}")
                signal[model_name] = None
        return signal
