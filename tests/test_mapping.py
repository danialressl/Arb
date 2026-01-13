import tempfile
import unittest

from arbscan.mapping import load_mappings


class MappingTests(unittest.TestCase):
    def test_load_mappings(self):
        content = """
        events:
          - canonical_event_id: "event-1"
            notes: "Test"
            venues:
              polymarket:
                market_id: "poly-1"
                yes_token_id: "poly-1-YES"
                no_token_id: "poly-1-NO"
              kalshi:
                ticker: "TEST-TICKER"
        """
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as tmp:
            tmp.write(content)
            tmp_path = tmp.name
        mapping_file = load_mappings(tmp_path)
        self.assertEqual(len(mapping_file.events), 1)
        self.assertEqual(mapping_file.events[0].canonical_event_id, "event-1")
        self.assertEqual(mapping_file.events[0].refs[0].venue, "polymarket")


if __name__ == "__main__":
    unittest.main()
