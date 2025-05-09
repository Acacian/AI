import tritonclient.http as httpclient
import numpy as np

TRITON_SERVER_URL = "triton:8000"

def send_to_triton(model_name: str, data: dict) -> list:
    client = httpclient.InferenceServerClient(url=TRITON_SERVER_URL)

    features = data.get("input")
    if not isinstance(features, list):
        raise ValueError("❌ Triton 입력 형식이 올바르지 않습니다.")

    # 입력 차원 정규화: [seq, dim] → [1, seq, dim]
    input_array = np.array(features, dtype=np.float32)
    if input_array.ndim == 2:
        input_array = np.expand_dims(input_array, axis=0)  # [1, seq_len, dim]

    inputs = [httpclient.InferInput("INPUT", input_array.shape, "FP32")]
    inputs[0].set_data_from_numpy(input_array)

    outputs = [httpclient.InferRequestedOutput("OUTPUT")]
    result = client.infer(model_name, inputs=inputs, outputs=outputs)

    return result.as_numpy("OUTPUT").tolist()
