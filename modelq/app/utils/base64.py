from PIL import Image
import io
import base64


def base64_to_image(base64_string):
    split = base64_string.split(",")
    if len(split) > 1:
        base64_data = split[1]
    else:

        base64_data = base64_string
    img = Image.open(io.BytesIO(base64.b64decode(base64_data)))

    return img
