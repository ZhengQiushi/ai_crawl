import time
from google import genai
from google.genai import types
from PIL import Image
from io import BytesIO
import base64
import json

client = genai.Client(api_key='AIzaSyDIrADHmfO8nT6rMKAOUTJVR6E-6f_QSRo')

with open('/home/azureuser/Trustbeez/crawl/output/test/0.json', 'r') as f:
    data = json.load(f)

for offering in data['offerings']:
    name = offering['offeringName']
    activity = offering['activity'].split(', ')[0]
    save_path = f"output/image/one/{name}.png"

    contents = f"Generate an poster about summer camp, focus on {activity}. Realistic style."
    print(contents)
    continue
    start = time.time()
    response = client.models.generate_content(
        model="gemini-2.0-flash-exp-image-generation",
        contents=contents,
        config=types.GenerateContentConfig(
        response_modalities=['Text', 'Image']
        )
    )
    end = time.time()
    print(f"Time taken: {end - start} seconds")

    if response.candidates[0].content:
        for part in response.candidates[0].content.parts:
            if part.text is not None:
                print(part.text)
            elif part.inline_data is not None:
                image = Image.open(BytesIO((part.inline_data.data)))
                image.save(save_path)
                image.show()
    else:
        print(contents)
 