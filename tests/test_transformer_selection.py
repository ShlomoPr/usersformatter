import unittest
from main import get_transformer_class_from_odata, ODATA_TRANSFORMER_MAP

class DummyTransformer:
    pass

class TestTransformerSelection(unittest.TestCase):
    def test_known_context(self):
        # Register a dummy transformer for a fake context
        ODATA_TRANSFORMER_MAP["dummy"] = DummyTransformer
        context = "https://graph.microsoft.com/beta/$metadata#dummy(...)"
        transformer_cls = get_transformer_class_from_odata(context)
        self.assertIs(transformer_cls, DummyTransformer)
        del ODATA_TRANSFORMER_MAP["dummy"]

    def test_default_fallback(self):
        # Should fallback to UserTransformer for unknown context
        from transformer import UserTransformer
        context = "https://graph.microsoft.com/beta/$metadata#unknown(...)"
        transformer_cls = get_transformer_class_from_odata(context)
        self.assertIs(transformer_cls, UserTransformer)

    def test_empty_context(self):
        from transformer import UserTransformer
        transformer_cls = get_transformer_class_from_odata("")
        self.assertIs(transformer_cls, UserTransformer)

if __name__ == "__main__":
    unittest.main()