#!/usr/bin/env python3
"""
Quick test to verify the status code fix logic
"""

# Simulate the responses that would come from the API functions
# These are examples based on the debug output you provided

def test_success_detection():
    """Test the new success detection logic"""

    # Example of successful responses (actual data from your debug output)
    successful_responses = [
        {
            'activities-steps': [{'dateTime': '2025-06-29', 'value': '1065'}],
            'activities-steps-intraday': {
                'dataset': [{'time': '00:00:00', 'value': 0}, {'time': '00:01:00', 'value': 5}],
                'datasetInterval': 1,
                'datasetType': 'minute'
            }
        }
    ]

    # Example of failed responses
    failed_responses_empty = []
    failed_responses_error = [{'error': 'Request failed with status code 502'}]
    failed_responses_492 = [{'error': '492'}]

    # Test the success detection logic
    def check_success(responses):
        return responses and any(
            isinstance(r, dict) and
            len(r) > 0 and
            'Request failed with status code' not in str(r) and
            '492' not in str(r)
            for r in responses
        )

    print("Testing success detection logic:")
    print(f"✓ Successful responses: {check_success(successful_responses)}")
    print(f"✗ Empty responses: {check_success(failed_responses_empty)}")
    print(f"✗ Error responses (502): {check_success(failed_responses_error)}")
    print(f"✗ Error responses (492): {check_success(failed_responses_492)}")

    print("\nResponse analysis:")
    for i, resp in enumerate(successful_responses):
        print(f"Response {i+1}:")
        print(f"  Type: {type(resp)}")
        print(f"  Keys: {list(resp.keys())}")
        print(f"  Length: {len(resp)}")
        print(f"  Contains error strings: {'Request failed with status code' in str(resp) or '492' in str(resp)}")

if __name__ == "__main__":
    test_success_detection()
