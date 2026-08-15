import os
import pytest
from dlq_resolver import get_llm_fix_code

def test_get_llm_fix_code_fallback(monkeypatch):
    """
    Test the agentic fallback logic when no API keys are provided.
    This ensures that the generated code compiles and executes correctly
    within the python sandbox.
    """
    # Ensure no API keys or models are set to trigger the fallback
    monkeypatch.delenv("OLLAMA_MODEL", raising=False)
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    
    error_msg = "test error"
    # Simulated Confluent Avro wire payload (5 byte header + JSON payload)
    raw_payload = b'\x00\x00\x00\x00\x01{"total_price": "100.50"}'
    schema_str = "{}"
    
    # Call the Agent method
    code = get_llm_fix_code(error_msg, raw_payload, schema_str)
    
    # Assert that the returned code contains the expected fallback logic
    assert "def fix_payload(raw_bytes):" in code
    assert "json.loads(json_str)" in code
    
    # Test executing the generated code safely in a sandbox (exactly as production does)
    local_env = {}
    exec(code, globals(), local_env)
    
    fix_func = local_env['fix_payload']
    assert callable(fix_func)
    
    # Actually run the generated code to ensure it works
    fixed_dict = fix_func(raw_payload)
    
    # Assert the mutation was successful
    assert 'total_price' in fixed_dict
    assert isinstance(fixed_dict['total_price'], float)
    assert fixed_dict['total_price'] == 100.50
