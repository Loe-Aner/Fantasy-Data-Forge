MODEL_PRICING = {
    "gpt-5.4": {
        "provider": "openai",
        "langchain_package": "langchain-openai",
        "langchain_class": "ChatOpenAI",
        "currency": "USD",
        "input_per_1m": 2.50,
        "output_per_1m": 15.00
    },
    "gpt-5.4-mini": {
        "provider": "openai",
        "langchain_package": "langchain-openai",
        "langchain_class": "ChatOpenAI",
        "currency": "USD",
        "input_per_1m": 0.75,
        "output_per_1m": 4.50
    },
    "gpt-5.4-nano": {
        "provider": "openai",
        "langchain_package": "langchain-openai",
        "langchain_class": "ChatOpenAI",
        "currency": "USD",
        "input_per_1m": 0.20,
        "output_per_1m": 1.25
    },
    "gpt-4.1": {
        "provider": "openai",
        "langchain_package": "langchain-openai",
        "langchain_class": "ChatOpenAI",
        "currency": "USD",
        "input_per_1m": 2.00,
        "output_per_1m": 8.00
    },
    "gpt-4.1-mini": {
        "provider": "openai",
        "langchain_package": "langchain-openai",
        "langchain_class": "ChatOpenAI",
        "currency": "USD",
        "input_per_1m": 0.40,
        "output_per_1m": 1.60
    },
    "gpt-4.1-nano": {
        "provider": "openai",
        "langchain_package": "langchain-openai",
        "langchain_class": "ChatOpenAI",
        "currency": "USD",
        "input_per_1m": 0.10,
        "output_per_1m": 0.40
    },
    "gemini-3.1-pro-preview": {
        "provider": "google",
        "langchain_package": "langchain-google-genai",
        "langchain_class": "ChatGoogleGenerativeAI",
        "currency": "USD",
        "input_per_1m": 2.00,
        "output_per_1m": 12.00
    },
    "gemini-3-flash-preview": {
        "provider": "google",
        "langchain_package": "langchain-google-genai",
        "langchain_class": "ChatGoogleGenerativeAI",
        "currency": "USD",
        "input_per_1m": 0.50,
        "output_per_1m": 3.00
    },
    "gemini-3.1-flash-lite-preview": {
        "provider": "google",
        "langchain_package": "langchain-google-genai",
        "langchain_class": "ChatGoogleGenerativeAI",
        "currency": "USD",
        "input_per_1m": 0.25,
        "output_per_1m": 1.50
    },
    "gemini-2.5-pro": {
        "provider": "google",
        "langchain_package": "langchain-google-genai",
        "langchain_class": "ChatGoogleGenerativeAI",
        "currency": "USD",
        "input_per_1m": 1.25,
        "output_per_1m": 10.00
    },
    "gemini-2.5-flash": {
        "provider": "google",
        "langchain_package": "langchain-google-genai",
        "langchain_class": "ChatGoogleGenerativeAI",
        "currency": "USD",
        "input_per_1m": 0.30,
        "output_per_1m": 1.0
    },
    "claude-opus-4-7": {
        "provider": "anthropic",
        "langchain_package": "langchain-anthropic",
        "langchain_class": "ChatAnthropic",
        "currency": "USD",
        "input_per_1m": 5.00,
        "output_per_1m": 25.00
    },
    "claude-opus-4-6": {
        "provider": "anthropic",
        "langchain_package": "langchain-anthropic",
        "langchain_class": "ChatAnthropic",
        "currency": "USD",
        "input_per_1m": 5.00,
        "output_per_1m": 25.00
    },
    "claude-sonnet-4-6": {
        "provider": "anthropic",
        "langchain_package": "langchain-anthropic",
        "langchain_class": "ChatAnthropic",
        "currency": "USD",
        "input_per_1m": 3.00,
        "output_per_1m": 15.00
    },
    "claude-haiku-4-5": {
        "provider": "anthropic",
        "langchain_package": "langchain-anthropic",
        "langchain_class": "ChatAnthropic",
        "currency": "USD",
        "input_per_1m": 1.00,
        "output_per_1m": 5.00
    },
    "qwen3.6-plus": {
        "provider": "qwen",
        "langchain_package": ["langchain-community", "langchain-qwq"],
        "langchain_class": ["ChatTongyi", "ChatQwen"],
        "currency": "CNY",
        "input_per_1m": 0.5,
        "output_per_1m": 3.0
    },
    "qwen3.5-397b-a17b": {
        "provider": "qwen",
        "langchain_package": ["langchain-community", "langchain-qwq"],
        "langchain_class": ["ChatTongyi", "ChatQwen"],
        "currency": "CNY",
        "input_per_1m": 0.6,
        "output_per_1m": 3.6
    },
    "qwen3.5-27b": {
        "provider": "qwen",
        "langchain_package": ["langchain-community", "langchain-qwq"],
        "langchain_class": ["ChatTongyi", "ChatQwen"],
        "currency": "CNY",
        "input_per_1m": 0.3,
        "output_per_1m": 2.4
    },
    "glm-5.1": {
        "provider": "glm",
        "langchain_package": ["langchain-community", "langchain-openai"],
        "langchain_class": ["ChatZhipuAI", "ChatOpenAI"],
        "currency": "CNY",
        "input_per_1m": 1.4,
        "output_per_1m": 4.4
    },
    "glm-5": {
        "provider": "glm",
        "langchain_package": ["langchain-community", "langchain-openai"],
        "langchain_class": ["ChatZhipuAI", "ChatOpenAI"],
        "currency": "CNY",
        "input_per_1m": 1.0,
        "output_per_1m": 3.2
    },
    "MiniMax-M2.7": {
        "provider": "minimax",
        "langchain_package": "langchain-community",
        "langchain_class": "MiniMaxChat",
        "currency": "USD",
        "input_per_1m": 0.30,
        "output_per_1m": 1.20
    },
    "kimi-k2.5": {
        "provider": "moonshot",
        "langchain_package": "langchain-moonshot",
        "langchain_class": "ChatMoonshot",
        "currency": "USD",
        "input_per_1m": 0.60,
        "output_per_1m": 3.00
    },
    "MiMo-V2-Pro": {
        "provider": "xiaomi_mimo",
        "langchain_package": None,
        "langchain_class": None,
        "langchain_supported": False,
        "currency": "USD",
        "input_per_1m": 1.00,
        "output_per_1m": 3.00
    }
}