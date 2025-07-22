import unittest
from transformer import UserTransformer

class TestUserTransformer(unittest.TestCase):
    def setUp(self):
        self.transformer = UserTransformer()

    def test_complete_user_transformation(self):
        """Given a complete user object, should return properly transformed user"""
        input_user = {
            "id": "12345-abcde-67890",
            "external_id": "ext-12345",
            "mail": "john.doe@example.com",
            "userType": "Member",
            "usageLocation": "US",
            "accountEnabled": True,
            "givenName": "John",
            "surname": "Doe",
            "signInActivity": {
                "lastSignInDateTime": "2023-10-01T12:00:00Z",
                "lastSignInRequestId": "req-12345",
                "lastNonInteractiveSignInDateTime": "2023-10-01T11:00:00Z",
                "lastNonInteractiveSignInRequestId": "req-67890",
                "lastSuccessfulSignInDateTime": "2023-10-01T10:00:00Z",
                "lastSuccessfulSignInRequestId": "req-abcde"
            }
        }
        
        expected_output = {
            "Id": "12345-abcde-67890",
            "external_id": "ext-12345",
            "mail": "john.doe@example.com",
            "type": "Member",
            "location": "US",
            "is_enabled": True,
            "first_name": "John",
            "last_name": "Doe",
            "signInActivity": {
                "lastSignIn": {
                    "dateTime": "2023-10-01T12:00:00Z",
                    "requestId": "req-12345"
                },
                "lastNonInteractiveSignIn": {
                    "dateTime": "2023-10-01T11:00:00Z",
                    "requestId": "req-67890"
                },
                "lastSuccessfulSignIn": {
                    "dateTime": "2023-10-01T10:00:00Z",
                    "requestId": "req-abcde"
                }
            }
        }
        
        result = self.transformer.transform(input_user)
        
        # Assert the complete transformation
        self.assertEqual(result["Id"], expected_output["Id"])
        self.assertEqual(result["external_id"], expected_output["external_id"])
        self.assertEqual(result["mail"], expected_output["mail"])
        self.assertEqual(result["type"], expected_output["type"])
        self.assertEqual(result["location"], expected_output["location"])
        self.assertEqual(result["is_enabled"], expected_output["is_enabled"])
        self.assertEqual(result["first_name"], expected_output["first_name"])
        self.assertEqual(result["last_name"], expected_output["last_name"])
        self.assertEqual(result["signInActivity"], expected_output["signInActivity"])

    def test_user_with_missing_external_id(self):
        """Given user without external_id, should return user with generated UUID"""
        input_user = {
            "id": "12345",
            "mail": "test@example.com"
        }
        
        result = self.transformer.transform(input_user)
        
        # Should generate a UUID for external_id
        self.assertIsNotNone(result["external_id"])
        self.assertIsInstance(result["external_id"], str)
        self.assertEqual(len(result["external_id"]), 36)  # UUID length
        self.assertIn("-", result["external_id"])  # UUID format

    def test_user_with_null_values(self):
        """Given user with null/None values, should return user with null values preserved"""
        input_user = {
            "id": "12345",
            "mail": None,
            "userType": None,
            "usageLocation": None,
            "accountEnabled": None,
            "givenName": None,
            "surname": None,
            "signInActivity": None
        }
        
        expected_output = {
            "Id": "12345",
            "mail": None,
            "type": None,
            "location": None,
            "is_enabled": None,
            "first_name": None,
            "last_name": None,
            "signInActivity": None
        }
        
        result = self.transformer.transform(input_user)
        
        self.assertEqual(result["Id"], expected_output["Id"])
        self.assertEqual(result["mail"], expected_output["mail"])
        self.assertEqual(result["type"], expected_output["type"])
        self.assertEqual(result["location"], expected_output["location"])
        self.assertEqual(result["is_enabled"], expected_output["is_enabled"])
        self.assertEqual(result["first_name"], expected_output["first_name"])
        self.assertEqual(result["last_name"], expected_output["last_name"])
        self.assertEqual(result["signInActivity"], expected_output["signInActivity"])

    def test_user_with_missing_fields(self):
        """Given user with missing fields, should return user with None values"""
        input_user = {
            "id": "12345"
        }
        
        result = self.transformer.transform(input_user)
        
        self.assertEqual(result["Id"], "12345")
        self.assertIsNotNone(result["external_id"])  # Generated UUID
        self.assertIsNone(result["mail"])
        self.assertIsNone(result["type"])
        self.assertIsNone(result["location"])
        self.assertIsNone(result["is_enabled"])
        self.assertIsNone(result["first_name"])
        self.assertIsNone(result["last_name"])
        self.assertIsNone(result["signInActivity"])

    def test_empty_user_object(self):
        """Given empty user object, should return user with None values and generated external_id"""
        input_user = {}
        
        result = self.transformer.transform(input_user)
        
        self.assertIsNone(result["Id"])
        self.assertIsNotNone(result["external_id"])  # Generated UUID
        self.assertIsNone(result["mail"])
        self.assertIsNone(result["type"])
        self.assertIsNone(result["location"])
        self.assertIsNone(result["is_enabled"])
        self.assertIsNone(result["first_name"])
        self.assertIsNone(result["last_name"])
        self.assertIsNone(result["signInActivity"])

    def test_signInActivity_with_partial_data(self):
        """Given signInActivity with only some fields, should return structured object with available data"""
        input_user = {
            "id": "12345",
            "signInActivity": {
                "lastSignInDateTime": "2023-10-01T12:00:00Z",
                "lastSignInRequestId": "req-12345"
                # Missing non-interactive and successful sign-in data
            }
        }
        
        expected_sign_in_activity = {
            "lastSignIn": {
                "dateTime": "2023-10-01T12:00:00Z",
                "requestId": "req-12345"
            },
            "lastNonInteractiveSignIn": {
                "dateTime": None,
                "requestId": None
            },
            "lastSuccessfulSignIn": {
                "dateTime": None,
                "requestId": None
            }
        }
        
        result = self.transformer.transform(input_user)
        
        self.assertEqual(result["signInActivity"], expected_sign_in_activity)

    def test_signInActivity_with_null_value(self):
        """Given signInActivity as None, should return None in the result"""
        input_user = {
            "id": "12345",
            "signInActivity": None
        }

        result = self.transformer.transform(input_user)

        self.assertIsNone(result["signInActivity"])

    def test_boolean_values_transformation(self):
        """Given user with various boolean values, should return proper boolean transformation"""
        test_cases = [
            (True, True),
            (False, False),
            ("true", "true"),  # String values should be preserved as-is
            ("false", "false"),
            (1, 1),  # Numeric values should be preserved as-is
            (0, 0)
        ]
        
        for input_value, expected_value in test_cases:
            with self.subTest(input_value=input_value):
                input_user = {
                    "id": "12345",
                    "accountEnabled": input_value
                }
                
                result = self.transformer.transform(input_user)
                
                self.assertEqual(result["is_enabled"], expected_value)

    def test_field_mapping_correctness(self):
        """Given user with Graph API field names, should return correctly mapped field names"""
        input_user = {
            "userType": "Guest",
            "usageLocation": "CA",
            "accountEnabled": False,
            "givenName": "Jane",
            "surname": "Smith"
        }
        
        result = self.transformer.transform(input_user)
        
        # Verify field name mapping
        self.assertEqual(result["type"], "Guest")
        self.assertEqual(result["location"], "CA")
        self.assertEqual(result["is_enabled"], False)
        self.assertEqual(result["first_name"], "Jane")
        self.assertEqual(result["last_name"], "Smith")
        
        # Verify original field names are not present
        self.assertNotIn("userType", result)
        self.assertNotIn("usageLocation", result)
        self.assertNotIn("accountEnabled", result)
        self.assertNotIn("givenName", result)
        self.assertNotIn("surname", result)

    def test_transform_with_invalid_input(self):
        """Should raise exception for invalid input"""
        transformer = UserTransformer()
        with self.assertRaises(Exception):
            transformer.transform(None)



if __name__ == "__main__":
    unittest.main()