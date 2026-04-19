from langchain_openai import ChatOpenAI
from dotenv import load_dotenv

load_dotenv()

TEMPERATURE_TRANSLATOR = 0.05
TEMPERATURE_EDITOR = 0.15

def llm_translator():
    llm = ChatOpenAI(
        model="gpt-5.4-nano",
        temperature=TEMPERATURE_TRANSLATOR,
        reasoning_effort="none",
        use_responses_api=True,
        max_retries=2
    )
    return llm

def llm_editor():
    llm = ChatOpenAI(
        model="gpt-5.4-mini",
        temperature=TEMPERATURE_EDITOR,
        reasoning_effort="none",
        use_responses_api=True,
        max_retries=2
    )
    return llm