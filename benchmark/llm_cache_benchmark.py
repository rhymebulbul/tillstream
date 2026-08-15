import time
import os
import sys
from unittest.mock import patch

# Add parent dir to path to import agents
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'agents')))
import dlq_resolver

class MockLLMResponse:
    @property
    def text(self):
        return "def fix_payload(raw_bytes):\n    return {}"

def mock_llm_call(*args, **kwargs):
    # Simulate network & generation latency of LLM (3 seconds)
    time.sleep(3.0)
    return MockLLMResponse()

@patch("dlq_resolver.genai.GenerativeModel.generate_content")
def run_benchmark(mock_generate):
    mock_generate.side_effect = mock_llm_call
    
    # Force the agent to use Gemini logic to trigger the mock
    os.environ["GEMINI_API_KEY"] = "benchmark_key"
    
    print("🚀 Starting LLM Semantic Cache Benchmark...")
    print("="*50)
    
    # 1. Measure Cache Miss (Network Bound LLM Call)
    print("⏳ Measuring Cache Miss (Network Bound LLM API Call)...")
    start = time.time()
    # Trigger first call (will hit mock and sleep 3s)
    dlq_resolver.get_llm_fix_code("ValueError: bad schema", b"raw1", "schema1")
    miss_time = (time.time() - start) * 1000
    print(f"❌ Cache Miss Latency: {miss_time:,.2f} ms")
    
    # 2. Measure Cache Hits (Memory Bound)
    print("\n⚡ Measuring 10,000 Cache Hits (Memory Bound Retrieval)...")
    hit_times = []
    
    # Run 10k iterations
    for _ in range(10000):
        start = time.time()
        # Same arguments will trigger the lru_cache immediately
        dlq_resolver.get_llm_fix_code("ValueError: bad schema", b"raw1", "schema1")
        hit_times.append((time.time() - start) * 1000)
    
    avg_hit = sum(hit_times) / len(hit_times)
    
    print(f"✅ Average Cache Hit Latency: {avg_hit:,.5f} ms")
    print(f"\n📈 PERFORMANCE GAIN: The cache is {miss_time / avg_hit:,.0f}x faster than the LLM API!")
    print("="*50)

if __name__ == "__main__":
    run_benchmark()
