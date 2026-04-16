from langchain_openai import ChatOpenAI
from dotenv import load_dotenv

load_dotenv()

TEMPERATURE_TRANSLATOR = 0.3
TEMPERATURE_EDITOR = 0.3

def llm_translator():
    llm = ChatOpenAI(
        model="gpt-5.4-nano",
        temperature=TEMPERATURE_TRANSLATOR,
        reasoning_effort="high",
        use_responses_api=True,
        max_retries=2
    )
    return llm