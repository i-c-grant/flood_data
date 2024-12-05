from abc import ABC, abstractmethod
from typing import Callable, Dict, Iterator, List, Tuple

import polars as pl


class BaseValidator(ABC):
    def _get_base_validations(self) -> Iterator[Tuple[str, pl.Expr]]:
        """Base validation expressions that apply to all data types"""
        # Subclasses can call super()._get_base_validations() to include these
        pass

    @abstractmethod
    def _get_validation_expressions(self) -> Iterator[Tuple[str, pl.Expr]]:
        """Yield tuples of (validation_name, validation_expression)"""
        pass

    def validate(self, df: pl.DataFrame) -> pl.DataFrame:
        # Collect all validation expressions and their names
        validations = list(self._get_validation_expressions())

        validated = df.with_columns(
                # Conduct validations
                (expr.alias(f"validation_{name}") for name, expr in validations),
                # Compute overall validity
                pl.all_horizontal(
                    [pl.col(f"validation_{name}") for name, _ in validations]
                ).alias("is_valid"),
                # Summarize and store validation results
                pl.struct(
                    [
                        pl.col(f"validation_{name}").alias(name)
                        for name, _ in validations
                    ]
                ).alias("validation_results")
        ).drop([f"validation_{name}" for name, _ in validations])

        return validated
