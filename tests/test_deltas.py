from datetime import timedelta

import isodate
import pytest

import pendulum

from k8s_snapshots import errors
from k8s_snapshots.rule import parse_deltas


@pytest.mark.parametrize(
    [
        'deltas',
        'expected_timedeltas',
    ],
    [
        pytest.param(
            'PT1M P1M',
            [
                isodate.Duration(minutes=1),
                isodate.Duration(months=1),
            ]
        ),
        pytest.param(
            'P7D P1D',
            [
                isodate.Duration(days=7),
                isodate.Duration(days=1),
            ]
        ),
        pytest.param(
            'PT1M PT7.5H P1M P5W P1Y',
            [
                isodate.Duration(minutes=1),
                isodate.Duration(hours=7.5),
                isodate.Duration(months=1),
                isodate.Duration(weeks=5),
                isodate.Duration(years=1),
            ],
        ),
        pytest.param(
            'PT1D PT1D',
            [],
            marks=pytest.mark.xfail(
                reason='T may only be used before time-based values such as '
                       'minute, hour, second',
                raises=errors.DeltasParseError,
                strict=True,
            )
        ),
        pytest.param(
            'PT1M',
            [],
            marks=pytest.mark.xfail(
                raises=errors.DeltasParseError,
                reason='Two deltas are required',
                strict=True,
            )
        ),
        pytest.param(
            'P1S P2S',
            [],
            marks=pytest.mark.xfail(
                raises=errors.DeltasParseError,
                reason='PT is required',
                strict=True
            )
        ),
        pytest.param(
            'pt2m',
            [],
            marks=pytest.mark.xfail(
                raises=errors.DeltasParseError,
                reason='ISO 8601 does not allow lowercase characters',
                strict=True
            )
        ),
        pytest.param(
            None,
            [],
            marks=pytest.mark.xfail(
                raises=errors.DeltasParseError,
                reason='deltas is None',
                strict=True,
            )
        )
    ]
)
def test_parse_deltas(deltas, expected_timedeltas):
    parsed_deltas = parse_deltas(deltas)
    assert parsed_deltas == expected_timedeltas
