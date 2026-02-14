import unittest

from arbv2.execution import _contracts_for_budget


class ExecutionBudgetTests(unittest.TestCase):
    def test_budget_caps_contracts(self) -> None:
        contracts = _contracts_for_budget(
            max_order_usd=5.0,
            signal_size=100.0,
            effective_prices=[0.4, 0.5],
        )
        self.assertEqual(contracts, 5)

    def test_signal_size_caps_contracts(self) -> None:
        contracts = _contracts_for_budget(
            max_order_usd=50.0,
            signal_size=3.0,
            effective_prices=[0.4, 0.5],
        )
        self.assertEqual(contracts, 3)

    def test_too_small_budget_returns_zero(self) -> None:
        contracts = _contracts_for_budget(
            max_order_usd=0.5,
            signal_size=100.0,
            effective_prices=[0.4, 0.5],
        )
        self.assertEqual(contracts, 0)


if __name__ == "__main__":
    unittest.main()
