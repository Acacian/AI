import tritonclient.http as httpclient
import numpy as np

TRITON_SERVER_URL = "triton:8000"

def send_to_triton(model_name: str, data: dict) -> list:
    client = httpclient.InferenceServerClient(url=TRITON_SERVER_URL)

    input_tensor = np.array([[data["close"]]], dtype=np.float32)

    inputs = [httpclient.InferInput("INPUT", input_tensor.shape, "FP32")]
    inputs[0].set_data_from_numpy(input_tensor)

    outputs = [httpclient.InferRequestedOutput("OUTPUT")]
    result = client.infer(model_name, inputs=inputs, outputs=outputs)

    return result.as_numpy("OUTPUT").tolist()
