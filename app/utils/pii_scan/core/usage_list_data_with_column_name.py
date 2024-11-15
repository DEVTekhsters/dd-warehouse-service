import json
from app.utils.pii_scan.structured_ner_main import MLBasedNERScannerForStructuredData
from typing import Dict, Any
def format_result_as_json(result: Dict[str, Any]) -> str:
    """
    Format the NER scan result as a JSON string.

    Args:
        result (Dict[str, Any]): NER scan result.

    Returns:
        str: JSON-formatted string of the result.
    """
    print(result, "input")
    formatted_result = {
        'entities': {
            entity: {
                'score': analysis.score,
                'appearances': analysis.appearances
            }
            for entity, analysis in result.get('entities', {}).items()
        },
        'sensitivity': result.get('sensitivity', {})
    }
    
    return json.dumps(formatted_result, indent=4)

def print_entities_and_sensitivity(result: Dict[str, Any]):
    """
    Print entities and sensitivity detection in a readable format.

    Args:
        result (Dict[str, Any]): NER scan result.
    """
    print("Entities:")
    try:
        entities = result.get('entities', {})
    except:
        result = eval(result)
        entities = result.get('entities', {})
    if entities:
        for entity, analysis in entities.items():
            # Print details of each entity
            print(f"Entity: {entity}, {{'score': {analysis.score}, 'appearances': {analysis.appearances}}}")
    else:
        print("  No entities detected.")

    print("\nSensitivity Detection:")
    sensitivity = result.get('sensitivity', {})
    if sensitivity:
        for sensitive_type, status in sensitivity.items():
            print(f"  Sensitive Type: {sensitive_type}")
            print(f"    Status: {status}")
    else:
        print("  No sensitivity detected.")

def main():
    scanner = MLBasedNERScannerForStructuredData()  # Initialize your NER Scanner

    # Expanded dataset with example entries
    data = [
        "Sample text with PII like SSN 123-45-6789",
        "Another text with a name John Doe",
        "Email address: john.doe@example.com",
        "Credit card number: 4111 1111 1111 1111",
        "Phone number: +1-800-555-1234",
        "Address: 1234 Elm Street, Springfield, IL 62704",
        "Date of birth: 01/01/1970",
        "Passport number: X1234567",
        "Bank account number: 9876543210",
        "Driver's license: D1234567",
        "Social Security Number: 987-65-4321",
        "Medical record number: MR123456",
        "Vehicle license plate: ABC1234",
        "Health insurance number: HI123456",
        "Employee ID: E123456",
        "Tax ID number: T123456789",
        "Student ID: S123456789",
        "Login credentials: user1234, passw0rd",
        "API key: 1234567890abcdef",
        "WiFi password: p@ssw0rd123",
        "Credit card CVV: 123",
    ]

    # Scan data with specified column_name
    column_name = "password"  # Example column_name for sensitivity detection
    result = scanner.scan(data, column_name=column_name)

    # Print results
    print_entities_and_sensitivity(result)

    # Print JSON-formatted results
    json_result = format_result_as_json(result)
    print("\nJSON Results:")
    print(json_result)

if __name__ == "__main__":
    main()
