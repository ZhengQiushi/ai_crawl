from google import genai
from google.genai.types import Tool, GenerateContentConfig, GoogleSearch

from google import genai
from google.genai import types

client = genai.Client(api_key="AIzaSyAcfhGrhtV_XDmzPPi8Zpm2Nf1V_I_hi2Y")

model_id = "gemini-2.0-flash"

google_search_tool = Tool(
    google_search = GoogleSearch()
)

response = client.models.generate_content(
    model=model_id,
    contents="""
            "http://www.ccfdmorristown.com/",
Does the agency offer summer camps for children in 2025? If yes, please provide a link to a specific summer camp. Please do not explain too much language, directly give the grounding URL characters, if not please give ""

Output Format:

Return the results as a String with ', ' as the separator. If no relevant summer camp pages, return "" without any explanations. Do not include any line breaks or special characters like \n. The related_urls should be sorted by relevance, with the most relevant pages listed first.

Output Example:
"https://example.com/summer-camp-2025, | , https://example.com/programs/summer-activities"
            """,
    config=GenerateContentConfig(
        tools=[google_search_tool],
        response_modalities=["TEXT"],
    )
)

for each in response.candidates[0].content.parts:
    print(each.text)
# Example response:
# The next total solar eclipse visible in the contiguous United States will be on ...

# To get grounding metadata as web content.
print(response.candidates[0].grounding_metadata.search_entry_point.rendered_content)
