"""
Bayesian daily sales estimator.

Combines multiple observable signals (Amazon monthly sales text, rating deltas,
BSR rank) into a posterior estimate of daily unit sales using MAP estimation
with Laplace approximation for confidence intervals.
"""

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
from scipy import stats
from scipy.misc import derivative
from scipy.optimize import minimize_scalar

logger = logging.getLogger(__name__)


@dataclass
class CategoryParams:
    """Per-category parameters calibrated from historical data."""
    review_rate_alpha: float = 2.0
    review_rate_beta: float = 80.0
    # ln(BSR) = -bsr_gamma * ln(daily_sales) + bsr_delta
    bsr_gamma: float = 0.8
    bsr_delta: float = 12.0
    bsr_sigma: float = 0.5


# Sensible defaults for common Amazon categories
DEFAULT_CATEGORY_PARAMS = CategoryParams()

_KNOWN_CATEGORY_PARAMS: Dict[str, CategoryParams] = {
    "coffee": CategoryParams(
        review_rate_alpha=2.5, review_rate_beta=90.0,
        bsr_gamma=0.75, bsr_delta=11.5, bsr_sigma=0.6,
    ),
    "electronics": CategoryParams(
        review_rate_alpha=3.0, review_rate_beta=100.0,
        bsr_gamma=0.85, bsr_delta=13.0, bsr_sigma=0.5,
    ),
}


def get_category_params(category_hint: Optional[str] = None) -> CategoryParams:
    if not category_hint:
        return DEFAULT_CATEGORY_PARAMS
    key = category_hint.lower().strip()
    for known, params in _KNOWN_CATEGORY_PARAMS.items():
        if known in key:
            return params
    return DEFAULT_CATEGORY_PARAMS


@dataclass
class SalesEstimate:
    estimated_daily_sales: int
    estimate_lower_bound: int
    estimate_upper_bound: int
    confidence_score: float
    estimation_method: str = "bayesian"


class BayesianDailySalesEstimator:
    """
    Estimate daily sales from sparse Amazon signals via Bayesian inference.

    Signals used:
    - sales_volume_num: Amazon's "X bought in past month" (coarse monthly)
    - num_ratings_delta: daily change in cumulative ratings (~1-3% of buyers rate)
    - bsr_rank: Best Seller Rank (log-linear relationship with sales)
    """

    def __init__(self, params: Optional[CategoryParams] = None):
        self.params = params or DEFAULT_CATEGORY_PARAMS

    def estimate(
        self,
        sales_volume_num: Optional[int] = None,
        num_ratings_delta: Optional[int] = None,
        bsr_rank: Optional[int] = None,
        prior_daily_sales: Optional[float] = None,
    ) -> SalesEstimate:
        p = self.params

        # --- Prior ---
        if prior_daily_sales and prior_daily_sales > 0:
            mu_prior = np.log(prior_daily_sales)
            sigma_prior = 0.5
        elif sales_volume_num and sales_volume_num > 0:
            mu_prior = np.log(max(sales_volume_num / 30.0, 0.5))
            sigma_prior = 0.8
        else:
            mu_prior = np.log(10)
            sigma_prior = 1.5

        def neg_log_posterior(log_s: float) -> float:
            s = np.exp(log_s)
            if s <= 0:
                return 1e10

            lp = stats.norm.logpdf(log_s, mu_prior, sigma_prior)

            if num_ratings_delta is not None and num_ratings_delta >= 0:
                avg_rr = p.review_rate_alpha / (p.review_rate_alpha + p.review_rate_beta)
                lam = max(s * avg_rr, 0.01)
                lp += stats.poisson.logpmf(num_ratings_delta, lam)

            if bsr_rank is not None and bsr_rank > 0:
                expected_ln_bsr = -p.bsr_gamma * log_s + p.bsr_delta
                lp += stats.norm.logpdf(np.log(bsr_rank), expected_ln_bsr, p.bsr_sigma)

            if sales_volume_num is not None and sales_volume_num > 0:
                monthly = s * 30
                lower_bound = sales_volume_num * 0.8
                upper_bound = sales_volume_num * 1.5
                if not (lower_bound <= monthly <= upper_bound):
                    penalty = ((monthly - sales_volume_num) / sales_volume_num) ** 2
                    lp -= penalty * 2

            return -lp

        result = minimize_scalar(
            neg_log_posterior,
            bounds=(np.log(0.1), np.log(100_000)),
            method="bounded",
        )
        map_log_s = result.x
        map_sales = np.exp(map_log_s)

        hessian = derivative(neg_log_posterior, map_log_s, n=2, dx=0.01)
        posterior_sigma = 1.0 / np.sqrt(max(hessian, 0.01))

        lower = np.exp(map_log_s - 1.96 * posterior_sigma)
        upper = np.exp(map_log_s + 1.96 * posterior_sigma)
        confidence = 1.0 / (1.0 + posterior_sigma)

        return SalesEstimate(
            estimated_daily_sales=max(int(round(map_sales)), 0),
            estimate_lower_bound=max(int(round(lower)), 0),
            estimate_upper_bound=max(int(round(upper)), 0),
            confidence_score=round(min(confidence, 1.0), 2),
        )


def calibrate_bsr_params(df: pd.DataFrame) -> Dict[str, float]:
    """
    Fit ln(BSR) = -gamma * ln(daily_sales) + delta from historical data.

    Expects DataFrame with columns: bsr_rank, sales_volume_num (monthly).
    Returns dict with bsr_gamma, bsr_delta, bsr_sigma.
    """
    df = df.dropna(subset=["bsr_rank", "sales_volume_num"])
    df = df[(df["bsr_rank"] > 0) & (df["sales_volume_num"] > 0)]

    if len(df) < 5:
        logger.warning("calibrate_bsr_params: insufficient data (%d rows), using defaults.", len(df))
        return {
            "bsr_gamma": DEFAULT_CATEGORY_PARAMS.bsr_gamma,
            "bsr_delta": DEFAULT_CATEGORY_PARAMS.bsr_delta,
            "bsr_sigma": DEFAULT_CATEGORY_PARAMS.bsr_sigma,
        }

    ln_bsr = np.log(df["bsr_rank"].values.astype(float))
    ln_sales = np.log(df["sales_volume_num"].values.astype(float) / 30.0)

    from sklearn.linear_model import LinearRegression
    model = LinearRegression().fit(ln_sales.reshape(-1, 1), ln_bsr)
    residuals = ln_bsr - model.predict(ln_sales.reshape(-1, 1))

    return {
        "bsr_gamma": float(-model.coef_[0]),
        "bsr_delta": float(model.intercept_),
        "bsr_sigma": float(np.std(residuals)),
    }


def estimate_daily_sales_batch(
    df: pd.DataFrame,
    category_hint: Optional[str] = None,
    params: Optional[CategoryParams] = None,
) -> pd.DataFrame:
    """
    Batch-estimate daily sales for a DataFrame.

    Input columns: asin, sales_volume_num, num_ratings_delta, bsr_rank (all optional).
    Adds columns: estimated_daily_sales, estimate_lower_bound, estimate_upper_bound,
                  confidence_score, estimation_method.
    """
    estimator = BayesianDailySalesEstimator(params or get_category_params(category_hint))

    results: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        est = estimator.estimate(
            sales_volume_num=_safe_int(row.get("sales_volume_num")),
            num_ratings_delta=_safe_int(row.get("num_ratings_delta")),
            bsr_rank=_safe_int(row.get("bsr_rank")),
            prior_daily_sales=_safe_float(row.get("prior_daily_sales")),
        )
        results.append({
            "estimated_daily_sales": est.estimated_daily_sales,
            "estimate_lower_bound": est.estimate_lower_bound,
            "estimate_upper_bound": est.estimate_upper_bound,
            "confidence_score": est.confidence_score,
            "estimation_method": est.estimation_method,
        })

    est_df = pd.DataFrame(results, index=df.index)
    return pd.concat([df, est_df], axis=1)


def _safe_int(val: Any) -> Optional[int]:
    if val is None:
        return None
    try:
        v = int(val)
        return v if not (isinstance(val, float) and np.isnan(val)) else None
    except (ValueError, TypeError):
        return None


def _safe_float(val: Any) -> Optional[float]:
    if val is None:
        return None
    try:
        v = float(val)
        return v if not np.isnan(v) else None
    except (ValueError, TypeError):
        return None
