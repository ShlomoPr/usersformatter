from abc import ABC, abstractmethod
import uuid
from datetime import datetime

class BaseTransformer(ABC):
    @abstractmethod
    def transform(self, user):
        """Transform a user record. Must be implemented by subclasses."""
        pass

class UserTransformer(BaseTransformer):
    def transform(self, user):
        return {
            "Id": user.get("id"),
            "external_id": user.get("external_id", str(uuid.uuid4())),
            "mail": user.get("mail"),
            "type": user.get("userType"),  # Fixed mapping
            "location": user.get("usageLocation"),  # Fixed mapping
            "is_enabled": user.get("accountEnabled"),  # Fixed mapping
            "first_name": user.get("givenName"),  # Fixed mapping
            "last_name": user.get("surname"),  # Fixed mapping
            "signInActivity": self.transform_sign_in_activity(user.get("signInActivity"))
        }

    def transform_sign_in_activity(self, activity):
        """Transform sign-in activity to a cleaner structure."""
        if not activity:
            return None

        return {
            "lastSignIn": {
                "dateTime": activity.get("lastSignInDateTime"),
                "requestId": activity.get("lastSignInRequestId")
            },
            "lastNonInteractiveSignIn": {
                "dateTime": activity.get("lastNonInteractiveSignInDateTime"),
                "requestId": activity.get("lastNonInteractiveSignInRequestId")
            },
            "lastSuccessfulSignIn": {
                "dateTime": activity.get("lastSuccessfulSignInDateTime"),
                "requestId": activity.get("lastSuccessfulSignInRequestId")
            }
        }