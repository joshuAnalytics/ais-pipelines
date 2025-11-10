"""AIS Vessel Type Mapping Utilities.

This module provides mappings and utilities for converting AIS vessel type codes
to human-readable descriptions.

Based on the ITU-R M.1371-5 standard for AIS vessel type codes (0-99).
"""

from typing import Dict, Optional
import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F


# Complete AIS vessel type code mapping
VESSEL_TYPES: Dict[int, str] = {
    0: "Not available",
    # 1-19: Reserved
    20: "Wing in ground (WIG)",
    21: "Wing in ground (WIG), Hazardous A",
    22: "Wing in ground (WIG), Hazardous B",
    23: "Wing in ground (WIG), Hazardous C",
    24: "Wing in ground (WIG), Hazardous D",
    30: "Fishing",
    31: "Towing",
    32: "Towing (large)",
    33: "Dredging or underwater ops",
    34: "Diving ops",
    35: "Military ops",
    36: "Sailing",
    37: "Pleasure Craft",
    40: "High speed craft (HSC)",
    41: "High speed craft (HSC), Hazardous A",
    42: "High speed craft (HSC), Hazardous B",
    43: "High speed craft (HSC), Hazardous C",
    44: "High speed craft (HSC), Hazardous D",
    49: "High speed craft (HSC), No additional info",
    50: "Pilot Vessel",
    51: "Search and Rescue",
    52: "Tug",
    53: "Port Tender",
    54: "Anti-pollution equipment",
    55: "Law Enforcement",
    56: "Spare - Local Vessel",
    57: "Spare - Local Vessel",
    58: "Medical Transport",
    59: "Noncombatant ship",
    60: "Passenger",
    61: "Passenger, Hazardous A",
    62: "Passenger, Hazardous B",
    63: "Passenger, Hazardous C",
    64: "Passenger, Hazardous D",
    69: "Passenger, No additional info",
    70: "Cargo",
    71: "Cargo, Hazardous A",
    72: "Cargo, Hazardous B",
    73: "Cargo, Hazardous C",
    74: "Cargo, Hazardous D",
    79: "Cargo, No additional info",
    80: "Tanker",
    81: "Tanker, Hazardous A",
    82: "Tanker, Hazardous B",
    83: "Tanker, Hazardous C",
    84: "Tanker, Hazardous D",
    89: "Tanker, No additional info",
    90: "Other Type",
    91: "Other Type, Hazardous A",
    92: "Other Type, Hazardous B",
    93: "Other Type, Hazardous C",
    94: "Other Type, Hazardous D",
    99: "Other Type, No additional info",
}


# Simplified categories for easier analysis
SIMPLIFIED_CATEGORIES: Dict[int, str] = {
    0: "Unknown",
    # WIG (20-29)
    **{i: "Wing in Ground" for i in range(20, 30)},
    # Fishing & Operations (30-39)
    30: "Fishing",
    31: "Towing",
    32: "Towing",
    33: "Dredging",
    34: "Diving ops",
    35: "Military",
    36: "Sailing",
    37: "Pleasure Craft",
    # High Speed Craft (40-49)
    **{i: "High Speed Craft" for i in range(40, 50)},
    # Service Vessels (50-59)
    50: "Pilot Vessel",
    51: "Search & Rescue",
    52: "Tug",
    53: "Port Tender",
    54: "Anti-pollution",
    55: "Law Enforcement",
    56: "Local Vessel",
    57: "Local Vessel",
    58: "Medical Transport",
    59: "Noncombatant",
    # Passenger (60-69)
    **{i: "Passenger" for i in range(60, 70)},
    # Cargo (70-79)
    **{i: "Cargo" for i in range(70, 80)},
    # Tanker (80-89)
    **{i: "Tanker" for i in range(80, 90)},
    # Other (90-99)
    **{i: "Other" for i in range(90, 100)},
}


def get_vessel_type_name(code: int, simplified: bool = True) -> str:
    """Convert vessel type code to human-readable name.
    
    Parameters
    ----------
    code : int
        AIS vessel type code (0-99)
    simplified : bool, default=True
        If True, return simplified category (e.g., "Cargo" instead of "Cargo, Hazardous A")
        If False, return detailed description
    
    Returns
    -------
    str
        Human-readable vessel type name
    
    Examples
    --------
    >>> get_vessel_type_name(70)
    'Cargo'
    >>> get_vessel_type_name(70, simplified=False)
    'Cargo, all ships of this type'
    >>> get_vessel_type_name(52)
    'Tug'
    """
    if simplified:
        return SIMPLIFIED_CATEGORIES.get(code, f"Unknown ({code})")
    return VESSEL_TYPES.get(code, f"Unknown ({code})")


def get_major_category(code: int) -> str:
    """Get major vessel category from code.
    
    Groups vessel types into major categories:
    - Cargo (70-79)
    - Tanker (80-89)
    - Passenger (60-69)
    - Fishing & Operations (30-39)
    - Service Vessels (50-59)
    - High Speed Craft (40-49)
    - Other
    
    Parameters
    ----------
    code : int
        AIS vessel type code (0-99)
    
    Returns
    -------
    str
        Major vessel category
    """
    if 70 <= code < 80:
        return "Cargo"
    elif 80 <= code < 90:
        return "Tanker"
    elif 60 <= code < 70:
        return "Passenger"
    elif 30 <= code < 40:
        return "Fishing & Operations"
    elif 50 <= code < 60:
        return "Service Vessels"
    elif 40 <= code < 50:
        return "High Speed Craft"
    elif 20 <= code < 30:
        return "Wing in Ground"
    elif 90 <= code < 100:
        return "Other"
    else:
        return "Unknown"


def map_vessel_types_pandas(
    df: pd.DataFrame,
    code_column: str = "vessel_type",
    simplified: bool = True,
    target_column: Optional[str] = None
) -> pd.DataFrame:
    """Map vessel type codes to names in a pandas DataFrame.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing vessel type codes
    code_column : str, default="vessel_type"
        Name of column containing vessel type codes
    simplified : bool, default=True
        Whether to use simplified category names
    target_column : str, optional
        Name for the new column with vessel type names.
        If None, will use "{code_column}_name"
    
    Returns
    -------
    pd.DataFrame
        DataFrame with added vessel type name column
    """
    if target_column is None:
        target_column = f"{code_column}_name"
    
    df = df.copy()
    df[target_column] = df[code_column].apply(
        lambda x: get_vessel_type_name(int(x), simplified=simplified) if pd.notna(x) else "Unknown"
    )
    return df


def map_vessel_types_spark(
    df: SparkDataFrame,
    code_column: str = "vessel_type",
    simplified: bool = True,
    target_column: Optional[str] = None
) -> SparkDataFrame:
    """Map vessel type codes to names in a Spark DataFrame.
    
    Parameters
    ----------
    df : SparkDataFrame
        Spark DataFrame containing vessel type codes
    code_column : str, default="vessel_type"
        Name of column containing vessel type codes
    simplified : bool, default=True
        Whether to use simplified category names
    target_column : str, optional
        Name for the new column with vessel type names.
        If None, will use "{code_column}_name"
    
    Returns
    -------
    SparkDataFrame
        DataFrame with added vessel type name column
    """
    if target_column is None:
        target_column = f"{code_column}_name"
    
    # Create mapping for Spark
    mapping_dict = SIMPLIFIED_CATEGORIES if simplified else VESSEL_TYPES
    
    # Create when conditions for all known codes
    mapping_expr = F.when(F.col(code_column).isNull(), "Unknown")
    for code, name in sorted(mapping_dict.items()):
        mapping_expr = mapping_expr.when(F.col(code_column) == code, name)
    mapping_expr = mapping_expr.otherwise(F.concat(F.lit("Unknown ("), F.col(code_column).cast("string"), F.lit(")")))
    
    return df.withColumn(target_column, mapping_expr)


def get_all_vessel_types(simplified: bool = True) -> Dict[int, str]:
    """Get dictionary of all vessel type codes and names.
    
    Parameters
    ----------
    simplified : bool, default=True
        Whether to return simplified or detailed names
    
    Returns
    -------
    Dict[int, str]
        Dictionary mapping vessel type codes to names
    """
    return SIMPLIFIED_CATEGORIES.copy() if simplified else VESSEL_TYPES.copy()
