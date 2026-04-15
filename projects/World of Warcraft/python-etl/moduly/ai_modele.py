from langchain_openai import ChatOpenAI

def llm_translator():
    llm = ChatOpenAI(
        model="gpt-5.4-nano",
        temperature=0.3,
        reasoning_effort="high",
        use_responses_api=True,
        max_retries=2
    )
    return llm