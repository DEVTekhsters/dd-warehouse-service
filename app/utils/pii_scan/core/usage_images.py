from app.utils.pii_scan.Octopii.octopii_pii_detector import process_file_octopii



result = process_file_octopii("dummy-pii/002.jpeg")
print(result)