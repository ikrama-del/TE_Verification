# ============================================================
# 📊 TEXTILE EXCHANGE
# ============================================================

import requests
import json
import pandas as pd
import asyncio
from playwright.async_api import async_playwright
from tabulate import tabulate
import os 
from datetime import datetime, timezone
# ------------------------------------------------------------
# AUTO TOKEN (Playwright)
# ------------------------------------------------------------
TARGET_URL = "https://textileexchange.org/find-certified-company/"
BASE_HEADERS = {
    "Content-Type": "application/json"
}
TOKEN_FILE = "token_TE.txt"

URL = "https://a9123a0b1c64468fa1e202bef9899172.pbidedicated.windows.net/webapi/capacities/A9123A0B-1C64-468F-A1E2-02BEF9899172/workloads/QES/QueryExecutionService/automatic/public/query"


# ============================================================
# TOKEN CAPTURE
# ============================================================
async def _extract_token_playwright():
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        page = await browser.new_page()

        token_found = None

        def intercept(req):
            nonlocal token_found
            if "/query" in req.url:
                auth = req.headers.get("authorization", "")
                if auth.startswith("MWCToken "):
                    token_found = auth

        page.on("request", intercept)
        await page.goto(TARGET_URL, timeout=500000)
        await page.wait_for_timeout(30000)
        await browser.close()

        return token_found


# ============================================================
# TOKEN STORAGE
# ============================================================
def generate_new_token():
    token = asyncio.run(_extract_token_playwright())
    if not token:
        raise Exception("❌ TOKEN NOT CAPTURED")

    with open(TOKEN_FILE, "w") as f:
        f.write(token)

    print("🔐 NEW TOKEN GENERATED")
    return token


def load_token():
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, "r") as f:
            return f.read().strip()
    return None


def get_token():
    token = load_token()
    if token:
        return token
    return generate_new_token()


# ============================================================
# AUTO TOKEN REQUEST
# ============================================================
def post_with_auto_token(url, headers=None, json=None):
    headers = dict(headers) if headers else {}

    headers.setdefault("Content-Type", "application/json")
    headers["Authorization"] = get_token()

    res = requests.post(url, headers=headers, json=json)

    if res.status_code == 401:
        print("⚠️ TOKEN EXPIRED → REGENERATING")
        token = generate_new_token()
        headers["Authorization"] = token
        res = requests.post(url, headers=headers, json=json)

    return res

# Open the HTML file in a headless browser and save a full-page screenshot
async def screenshot_html(html_path, output_png):
        
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)

        page = await browser.new_page(
            viewport={"width": 1400, "height": 900},
            device_scale_factor=2 
        )

        await page.goto(
            "file://" + os.path.abspath(html_path),
            wait_until="networkidle"
        )

        await page.screenshot(
            path=output_png,
            full_page=True
        )

        await browser.close()


# ------------------------------------------------------------
# USER INPUT
# ------------------------------------------------------------
sc_number = input("➡️ Enter SC Number: ").strip()


# ------------------------------------------------------------
# SC Header Table
# ------------------------------------------------------------

payload = {
    "version": "1.0.0",
    "allowLongRunningQueries": True,
    "modelId": 4668616,
    "queries": [
        {
            "QueryId": "",
            "ApplicationContext": {
                "DatasetId": "30385b55-d526-401d-8379-6a502d67c719",
                "Sources": [
                    {
                        "ReportId": "5bbb73cf-c2c7-4c8d-8461-f04b75a9438b",
                        "VisualId": "8c0cdf793984740d7703",
                    }
                ],
            },
            "Query": {
                "Commands": [
                    {
                        "SemanticQueryDataShapeCommand": {
                            "Query": {
                                "Version": 2,
                                "From": [
                                    {
                                        "Name": "s1",
                                        "Entity": "semantic vw_ScopeCertificate_SuppyChainOperator",
                                        "Type": 0,
                                    },
                                    {
                                        "Name": "s",
                                        "Entity": "semantic vw_ScopeCertificateStandard",
                                        "Type": 0,
                                    },
                                ],
                                "Select": [
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "s1"}
                                            },
                                            "Property": "CB_Name",
                                        }
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "s1"}
                                            },
                                            "Property": "SCO_Name",
                                        }
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "s1"}
                                            },
                                            "Property": "CO_Te_Id",
                                        }
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "s1"}
                                            },
                                            "Property": "Retired_CO_Te_Id",
                                        }
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "s1"}
                                            },
                                            "Property": "SCO_License",
                                        }
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "s1"}
                                            },
                                            "Property": "SCO_Native_Name",
                                        }
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "s1"}
                                            },
                                            "Property": "SCO_Country",
                                        }
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "s1"}
                                            },
                                            "Property": "SC_Number",
                                        }
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "s1"}
                                            },
                                            "Property": "SC_Version_No",
                                        }
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "s"}
                                            },
                                            "Property": "Grouped SC Standard",
                                        }
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "s1"}
                                            },
                                            "Property": "SC_Status",
                                        }
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "s1"}
                                            },
                                            "Property": "SC_Last_Updated_Date",
                                        }
                                    },
                                ],
                                "Where": [
                                    {
                                        "Condition": {
                                            "Contains": {
                                                "Left": {
                                                    "Column": {
                                                        "Expression": {
                                                            "SourceRef": {
                                                                "Source": "s1"
                                                            }
                                                        },
                                                        "Property": "SC Number | SC Version No",
                                                    }
                                                },
                                                "Right": {
                                                    "Literal": {
                                                        "Value": f"'{sc_number}'"
                                                    }
                                                },
                                            }
                                        }
                                    },
                                ],
                            },
                            "Binding": {
                                "Primary": {
                                    "Groupings": [
                                        {
                                            "Projections": [
                                                0,
                                                1,
                                                2,
                                                3,
                                                4,
                                                5,
                                                6,
                                                7,
                                                8,
                                                9,
                                                10,
                                                11,
                                            ]
                                        }
                                    ]
                                },
                                "DataReduction": {
                                    "DataVolume": 3,
                                    "Primary": {"Window": {"Count": 500}},
                                },
                                "Version": 1,
                            },
                        }
                    }
                ]
            },
        }
    ],
}


# REQUEST 
print("\n📡 Fetching SC Header Table...")

res = post_with_auto_token(URL, headers=BASE_HEADERS, json=payload)

if res.status_code != 200:
    print("❌ ERROR:", res.text)
    exit()

js = res.json()


# ==========================================
# DECODE SC Header Table
# ==========================================

def decode(js):
    try:
        ds = js["results"][0]["result"]["data"]["dsr"]["DS"][0]
    except:
        return []

    vd = ds.get("ValueDicts", {})
    rows_raw = ds["PH"][0]["DM0"]

    rows = []

    def get(dict_name, idx):
        try:
            return vd[dict_name][idx]
        except:
            return None

    for row in rows_raw:
        C = row["C"]

        # ----------------------------
        # dernier élément du array C
        # ----------------------------
        ts = C[-1] if isinstance(C[-1], int) else None

        if ts:
            last_update = datetime.utcfromtimestamp(ts / 1000).strftime("%Y-%m-%d")
        else:
            last_update = None

        rows.append(
            [
                get("D0", C[0]),
                get("D1", C[1]),
                get("D2", C[2]),
                get("D3", C[3]),
                get("D4", C[4]),
                get("D5", C[5]),
                get("D6", C[6]),
                get("D7", C[7]),
                get("D8", 0),
                get("D9", 0),
                get("D10", 0),
                last_update,
            ]
        )

    return rows



# DATAFRAME
rows = decode(js)
df_sc = pd.DataFrame(
    rows,
    columns=[
        "Certification Body",
        "Certified Organization Name",
        "TE ID",
        "Retired TE ID",
        "License Number",
        "Native Name",
        "Country/Area",
        "SC Number",
        "SC Version Number",
        "Standard",
        "SC Status",
        "LastUpdated_Timestamp",
    ],
)

print("✅ SC HEADER DATA EXTRACTED SUCCESSFULLY")

# First SC result row
row = df_sc.iloc[0]

def lit(v):
    # Null value
    if v is None or pd.isna(v) or v == "null":
        return "null"

    # Empty string
    if isinstance(v, str) and v.strip() == "":
        return "''"
    
    # Normal value
    return f"'{v}'"


def lit_date(v):
    if pd.isna(v) or v in ["", None, "null"]:
        return "null"
    return f"datetime'{v}T00:00:00'"


# ============================================================
# PRODUCT
# ============================================================

payload_products = {
    "version": "1.0.0",
    "allowLongRunningQueries": True,
    "modelId": 4668616,
    "queries": [
        {
            "QueryId": "",
            "userPreferredLocale": "en",
            "ApplicationContext": {
                "DatasetId": "30385b55-d526-401d-8379-6a502d67c719",
                "Sources": [
                    {
                        "ReportId": "5bbb73cf-c2c7-4c8d-8461-f04b75a9438b",
                        "VisualId": "637e2cd9d391deed6466",
                    }
                ],
            },
            "Query": {
                "Commands": [
                    {
                        "SemanticQueryDataShapeCommand": {
                            "ExecutionMetricsKind": 1,
                            "Query": {
                                "Version": 2,
                                "From": [
                                    {
                                        "Name": "s1",
                                        "Entity": "semantic vw_facility",
                                        "Type": 0,
                                    },
                                    {
                                        "Name": "s2",
                                        "Entity": "semantic vw_Products",
                                        "Type": 0,
                                    },
                                    {
                                        "Name": "s11",
                                        "Entity": "semantic vw_ScopeCertificate_SuppyChainOperator",
                                        "Type": 0,
                                    },
                                    {
                                        "Name": "s21",
                                        "Entity": "semantic vw_ScopeCertificateStandard",
                                        "Type": 0,
                                    },
                                ],
                                "Select": [
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "s2"}
                                            },
                                            "Property": "Product_Category_Desc",
                                        }
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "s2"}
                                            },
                                            "Property": "Product_Category_Code",
                                        }
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "s2"}
                                            },
                                            "Property": "Product_Detail_Desc",
                                        }
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "s2"}
                                            },
                                            "Property": "Product_Detail_Code",
                                        }
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "s2"}
                                            },
                                            "Property": "Raw Matetrial Description",
                                        }
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "s2"}
                                            },
                                            "Property": "Raw_Material_Percentage",
                                        }
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "s1"}
                                            },
                                            "Property": "Facility Name",
                                        }
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "s2"}
                                            },
                                            "Property": "Raw_Material_Code",
                                        }
                                    },
                                ],
                        
                                "Where": [
                                    
                                    {
                                        "Condition": {
                                            "In": {
                                                "Expressions": [
                                                    {
                                                        "Column": {
                                                            "Expression": {
                                                                "SourceRef": {
                                                                    "Source": "s1"
                                                                }
                                                            },
                                                            "Property": "facility_type",
                                                        }
                                                    }
                                                ],
                                                "Values": [
                                                    [
                                                        {
                                                            "Literal": {
                                                                "Value": "'Subsequent'"
                                                            }
                                                        }
                                                    ],
                                                    [
                                                        {
                                                            "Literal": {
                                                                "Value": "'Main Facility'"
                                                            }
                                                        }
                                                    ],
                                                ],
                                            }
                                        }
                                    },
                                    {
                                        "Condition": {
                                            "In": {
                                                "Expressions": [
                                                    {
                                                        "Column": {
                                                            "Expression": {
                                                                "SourceRef": {
                                                                    "Source": "s11"
                                                                }
                                                            },
                                                            "Property": "SCO_Name",
                                                        }
                                                    }
                                                ],
                                                "Values": [
                                                    [
                                                        {
                                                            "Literal": {
                                                                "Value": lit(
                                                                    row[
                                                                        "Certified Organization Name"].replace("'", "''")
                                                                    
                                                                )
                                                            }
                                                        }
                                                    ]
                                                ],
                                            }
                                        }
                                    },
                                    {
                                        "Condition": {
                                            "In": {
                                                "Expressions": [
                                                    {
                                                        "Column": {
                                                            "Expression": {
                                                                "SourceRef": {
                                                                    "Source": "s11"
                                                                }
                                                            },
                                                            "Property": "SCO_Country",
                                                        }
                                                    }
                                                ],
                                                "Values": [
                                                    [
                                                        {
                                                            "Literal": {
                                                                "Value": lit(
                                                                    row["Country/Area"]
                                                                )
                                                            }
                                                        }
                                                    ]
                                                ],
                                            }
                                        }
                                    },
                                    {
                                        "Condition": {
                                            "In": {
                                                "Expressions": [
                                                    {
                                                        "Column": {
                                                            "Expression": {
                                                                "SourceRef": {
                                                                    "Source": "s11"
                                                                }
                                                            },
                                                            "Property": "SCO_License",
                                                        }
                                                    }
                                                ],
                                                "Values": [
                                                    [
                                                        {
                                                            "Literal": {
                                                                "Value": lit(
                                                                    row[
                                                                        "License Number"
                                                                    ]
                                                                )
                                                            }
                                                        }
                                                    ]
                                                ],
                                            }
                                        }
                                    },
                                    {
                                        "Condition": {
                                            "In": {
                                                "Expressions": [
                                                    {
                                                        "Column": {
                                                            "Expression": {
                                                                "SourceRef": {
                                                                    "Source": "s11"
                                                                }
                                                            },
                                                            "Property": "SCO_Native_Name",
                                                        }
                                                    }
                                                ],
                                                "Values": [
                                                    [
                                                        {
                                                            "Literal": {
                                                                "Value": (
                                                                    "''"
                                                                    if row[
                                                                        "Native Name"
                                                                    ]
                                                                    == ""
                                                                    else lit(
                                                                        row[
                                                                            "Native Name"
                                                                        ]
                                                                    )
                                                                )
                                                            }
                                                        }
                                                    ]
                                                ],
                                            }
                                        }
                                    },
                                    {
                                        "Condition": {
                                            "In": {
                                                "Expressions": [
                                                    {
                                                        "Column": {
                                                            "Expression": {
                                                                "SourceRef": {
                                                                    "Source": "s11"
                                                                }
                                                            },
                                                            "Property": "SC_Number",
                                                        }
                                                    }
                                                ],
                                                "Values": [
                                                    [
                                                        {
                                                            "Literal": {
                                                                "Value": lit(
                                                                    row["SC Number"]
                                                                )
                                                            }
                                                        }
                                                    ]
                                                ],
                                            }
                                        }
                                    },
                                    {
                                        "Condition": {
                                            "In": {
                                                "Expressions": [
                                                    {
                                                        "Column": {
                                                            "Expression": {
                                                                "SourceRef": {
                                                                    "Source": "s11"
                                                                }
                                                            },
                                                            "Property": "SC_Status",
                                                        }
                                                    }
                                                ],
                                                "Values": [
                                                    [
                                                        {
                                                            "Literal": {
                                                                "Value": lit(
                                                                    row["SC Status"]
                                                                )
                                                            }
                                                        }
                                                    ]
                                                ],
                                            }
                                        }
                                    },
                                    {
                                        "Condition": {
                                            "In": {
                                                "Expressions": [
                                                    {
                                                        "Column": {
                                                            "Expression": {
                                                                "SourceRef": {
                                                                    "Source": "s11"
                                                                }
                                                            },
                                                            "Property": "Retired_CO_Te_Id",
                                                        }
                                                    }
                                                ],
                                                "Values": [
                                                    [
                                                        {
                                                            "Literal": {
                                                                "Value": lit(
                                                                    row["Retired TE ID"]
                                                                )
                                                            }
                                                        }
                                                    ]
                                                ],
                                            }
                                        }
                                    },
                                    {
                                        "Condition": {
                                            "In": {
                                                "Expressions": [
                                                    {
                                                        "Column": {
                                                            "Expression": {
                                                                "SourceRef": {
                                                                    "Source": "s21"
                                                                }
                                                            },
                                                            "Property": "Grouped SC Standard",
                                                        }
                                                    }
                                                ],
                                                "Values": [
                                                    [
                                                        {
                                                            "Literal": {
                                                                "Value": lit(
                                                                    row["Standard"]
                                                                )
                                                            }
                                                        }
                                                    ]
                                                ],
                                            }
                                        }
                                    },
                                    {
                                        "Condition": {
                                            "Contains": {
                                                "Left": {
                                                    "Column": {
                                                        "Expression": {
                                                            "SourceRef": {
                                                                "Source": "s11"
                                                            }
                                                        },
                                                        "Property": "SC Number | SC Version No",
                                                    }
                                                },
                                                "Right": {
                                                    "Literal": {
                                                        "Value": lit(row["SC Number"])
                                                    }
                                                },
                                            }
                                        }
                                    },
                                    {
                                        "Condition": {
                                            "In": {
                                                "Expressions": [
                                                    {
                                                        "Column": {
                                                            "Expression": {
                                                                "SourceRef": {
                                                                    "Source": "s11"
                                                                }
                                                            },
                                                            "Property": "CB_Name",
                                                        }
                                                    }
                                                ],
                                                "Values": [
                                                    [
                                                        {
                                                            "Literal": {
                                                                "Value": lit(
                                                                    row[
                                                                        "Certification Body"
                                                                    ]
                                                                )
                                                            }
                                                        }
                                                    ]
                                                ],
                                            }
                                        }
                                    },
                                    {
                                        "Condition": {
                                            "In": {
                                                "Expressions": [
                                                    {
                                                        "Column": {
                                                            "Expression": {
                                                                "SourceRef": {
                                                                    "Source": "s11"
                                                                }
                                                            },
                                                            "Property": "CO_Te_Id",
                                                        }
                                                    }
                                                ],
                                                "Values": [
                                                    [
                                                        {
                                                            "Literal": {
                                                                "Value": lit(
                                                                    row["TE ID"]
                                                                )
                                                            }
                                                        }
                                                    ]
                                                ],
                                            }
                                        }
                                    },
                                    {
                                        "Condition": {
                                            "In": {
                                                "Expressions": [
                                                    {
                                                        "Column": {
                                                            "Expression": {
                                                                "SourceRef": {
                                                                    "Source": "s11"
                                                                }
                                                            },
                                                            "Property": "SC_Version_No",
                                                        }
                                                    }
                                                ],
                                                "Values": [
                                                    [
                                                        {
                                                            "Literal": {
                                                                "Value": lit(
                                                                    row[
                                                                        "SC Version Number"
                                                                    ]
                                                                )
                                                            }
                                                        }
                                                    ]
                                                ],
                                            }
                                        }
                                    },
                                    {
                                        "Condition": {
                                            "In": {
                                                "Expressions": [
                                                    {
                                                        "Column": {
                                                            "Expression": {
                                                                "SourceRef": {
                                                                    "Source": "s11"
                                                                }
                                                            },
                                                            "Property": "SC_Last_Updated_Date",
                                                        }
                                                    }
                                                ],
                                                "Values": [
                                                    [
                                                        {
                                                            "Literal": {
                                                                "Value": lit_date(
                                                                    row[
                                                                        "LastUpdated_Timestamp"
                                                                    ]
                                                                )
                                                            }
                                                        }
                                                    ]
                                                ],
                                            }
                                        }
                                    },
                                    {
                                        "Condition": {
                                            "And": {
                                                "Left": {
                                                    "Not": {
                                                        "Expression": {
                                                            "Comparison": {
                                                                "ComparisonKind": 0,
                                                                "Left": {
                                                                    "Column": {
                                                                        "Expression": {
                                                                            "SourceRef": {
                                                                                "Source": "s11"
                                                                            }
                                                                        },
                                                                        "Property": "SC_Number",
                                                                    }
                                                                },
                                                                "Right": {
                                                                    "Literal": {
                                                                        "Value": "'CU1163654MUL-2023-00034573'"
                                                                    }
                                                                },
                                                            }
                                                        }
                                                    }
                                                },
                                                "Right": {
                                                    "Not": {
                                                        "Expression": {
                                                            "Comparison": {
                                                                "ComparisonKind": 0,
                                                                "Left": {
                                                                    "Column": {
                                                                        "Expression": {
                                                                            "SourceRef": {
                                                                                "Source": "s11"
                                                                            }
                                                                        },
                                                                        "Property": "SC_Number",
                                                                    }
                                                                },
                                                                "Right": {
                                                                    "Literal": {
                                                                        "Value": "'CU807457MUL-2024-00115951'"
                                                                    }
                                                                },
                                                            }
                                                        }
                                                    }
                                                },
                                            }
                                        }
                                    },
                                    {
                                        "Condition": {
                                            "Not": {
                                                "Expression": {
                                                    "Comparison": {
                                                        "ComparisonKind": 0,
                                                        "Left": {
                                                            "Column": {
                                                                "Expression": {
                                                                    "SourceRef": {
                                                                        "Source": "s11"
                                                                    }
                                                                },
                                                                "Property": "CB_Code",
                                                            }
                                                        },
                                                        "Right": {
                                                            "Literal": {
                                                                "Value": "'CB-DCB'"
                                                            }
                                                        },
                                                    }
                                                }
                                            }
                                        }
                                    },
                                    {
                                        "Condition": {
                                            "And": {
                                                "Left": {
                                                    "Not": {
                                                        "Expression": {
                                                            "Comparison": {
                                                                "ComparisonKind": 0,
                                                                "Left": {
                                                                    "Column": {
                                                                        "Expression": {
                                                                            "SourceRef": {
                                                                                "Source": "s11"
                                                                            }
                                                                        },
                                                                        "Property": "SC_Number",
                                                                    }
                                                                },
                                                                "Right": {
                                                                    "Literal": {
                                                                        "Value": "'CU807457RAF-2024-000801115'"
                                                                    }
                                                                },
                                                            }
                                                        }
                                                    }
                                                },
                                                "Right": {
                                                    "Not": {
                                                        "Expression": {
                                                            "Comparison": {
                                                                "ComparisonKind": 0,
                                                                "Left": {
                                                                    "Column": {
                                                                        "Expression": {
                                                                            "SourceRef": {
                                                                                "Source": "s11"
                                                                            }
                                                                        },
                                                                        "Property": "SC_Number",
                                                                    }
                                                                },
                                                                "Right": {
                                                                    "Literal": {
                                                                        "Value": "'CU1042889MUL-2024-00027504'"
                                                                    }
                                                                },
                                                            }
                                                        }
                                                    }
                                                },
                                            }
                                        }
                                    },
                                ], 
                            },
                            "Binding": {
                                "Primary": {
                                    "Groupings": [
                                        {"Projections": [6], "Subtotal": 0},
                                        {"Projections": [1], "Subtotal": 0},
                                        {"Projections": [0], "Subtotal": 0},
                                        {"Projections": [3], "Subtotal": 0},
                                        {"Projections": [2], "Subtotal": 0},
                                        {"Projections": [7], "Subtotal": 0},
                                        {"Projections": [4], "Subtotal": 0},
                                        {"Projections": [5], "Subtotal": 0},
                                    ]
                                },
                                "DataReduction": {
                                    "DataVolume": 3,
                                    "Primary": {"Window": {"Count": 500}},
                                },
                                "Version": 1,
                            },
                        }
                    }
                ]
            },
        }
    ],
}


# REQUEST
print("\n📡 Fetching PRODUCT DATA...")

res_prod = post_with_auto_token(URL, headers=BASE_HEADERS, json=payload_products)

if res_prod.status_code != 200:
    print("❌ ERROR PRODUCT:", res_prod.text)
    exit()

js_prod = res_prod.json()


# ==========================================
# DECODE PRODUCTS 
# ==========================================

def extract_products(js):

    ds = js["results"][0]["result"]["data"]["dsr"]["DS"][0]
    vd = ds["ValueDicts"]

    def vdict(name, idx):
        try:
            return vd[name][idx]
        except:
            return None

    rows = []

    
    last = {
        "facility": None,
        "cat_code": None,
        "cat_desc": None,
        "detail_code": None,
        "detail_desc": None,
        "raw_code": None,
        "raw_desc": None,
        "raw_pct": None,
    }

    def walk(node, ctx):
        nonlocal last
        ctx = ctx.copy()

        # FACILITY
        if "G0" in node:
            val = node["G0"]
            ctx["facility"] = val
            last["facility"] = val

        # CATEGORY
        if "G1" in node:
            val = vdict("D0", node["G1"])
            ctx["cat_code"] = val
            last["cat_code"] = val

        if "G2" in node:
            val = vdict("D1", node["G2"])
            ctx["cat_desc"] = val
            last["cat_desc"] = val

        # DETAIL
        if "G3" in node:
            val = vdict("D2", node["G3"])
            ctx["detail_code"] = val
            last["detail_code"] = val

        if "G4" in node:
            val = vdict("D3", node["G4"])
            ctx["detail_desc"] = val
            last["detail_desc"] = val

        # RAW MATERIAL
        if "G5" in node:
            val = vdict("D4", node["G5"])
            ctx["raw_code"] = val
            last["raw_code"] = val

        # RAW DESC
        if "G6" in node:
            val = vdict("D5", node["G6"])
            ctx["raw_desc"] = val
            last["raw_desc"] = val

        # RAW %
        if "G7" in node:
            val = vdict("D6", node["G7"])
            ctx["raw_pct"] = val
            last["raw_pct"] = val

     
        if node.get("R") == 1:
            ctx.update(last)


        if "M" not in node:
            rows.append(
                    {
                        "Facility Name": last["facility"],
                        "Product Category Code": last["cat_code"],
                        "Product Category Description": last["cat_desc"],
                        "Product Detail Code": last["detail_code"],
                        "Product Detail Description": last["detail_desc"],
                        "Raw Material Code": last["raw_code"],
                        "Raw Material Description": last["raw_desc"],
                        "Raw Material Percentage": last["raw_pct"],
                    }
                )
            return


        for block in node["M"]:
            for dm, children in block.items():  
                for child in children:
                    walk(child, ctx)


    for root in ds["PH"][0]["DM0"]:
        walk(root, {})

    return rows


rows = extract_products(js_prod)

df = pd.DataFrame(rows)

df = df.rename(
    columns={
        "facility": "Facility Name",
        "cat_code": "Product Category Code",
        "cat_desc": "Product Category Description",
        "detail_code": "Product Detail Code",
        "detail_desc": "Product Detail Description",
        "raw_code": "Raw Material Code",
        "raw_desc": "Raw Material Description",
        "percentage": "Raw Material Percentage",
    }
)

df_products = df[
    [
        "Facility Name",
        "Product Category Code",
        "Product Category Description",
        "Product Detail Code",
        "Product Detail Description",
        "Raw Material Code",
        "Raw Material Description",
        "Raw Material Percentage",
    ]
]


print("✅ Product DATA EXTRACTED SUCCESSFULLY")

# First Product row
row_prod = df_products.iloc[0]


def lit_P(v):
    if pd.isna(v) or v in ["", None, "null"]:
        return "null"
    return f"'{v}'"


# -----------------------------
# Facility
# -----------------------------
payload_Facility = {
    "version": "1.0.0",
    "allowLongRunningQueries": True,
    "modelId": 4668616,
    "queries": [
        {
            "QueryId": "",
            "userPreferredLocale": "en",
            "ApplicationContext": {
                "DatasetId": "30385b55-d526-401d-8379-6a502d67c719",
                "Sources": [
                    {
                        "ReportId": "5bbb73cf-c2c7-4c8d-8461-f04b75a9438b",
                        "VisualId": "9dcd0600752094910de4",
                    }
                ],
            },
            "Query": {
                "Commands": [
                    {
                        "SemanticQueryDataShapeCommand": {
                            "ExecutionMetricsKind": 1,
                            "Query": {
                                "Version": 2,
                                "From": [
                                    {
                                        "Name": "s1",
                                        "Entity": "semantic vw_facility",
                                        "Type": 0,
                                    },
                                    {
                                        "Name": "s2",
                                        "Entity": "semantic vw_FacilityStandard",
                                        "Type": 0,
                                    },
                                    {
                                        "Name": "s3",
                                        "Entity": "semantic vw_Process",
                                        "Type": 0,
                                    },
                                    {
                                        "Name": "s",
                                        "Entity": "semantic vw_ScopeCertificate_SuppyChainOperator",
                                        "Type": 0,
                                    },
                                    {
                                        "Name": "s21",
                                        "Entity": "semantic vw_ScopeCertificateStandard",
                                        "Type": 0,
                                    },
                                ],
                    
                                "Select": [
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "s1"}
                                            },
                                            "Property": "facility_type",
                                        },
                                        "Name": "semantic vw_facility.facility_type",
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "s2"}
                                            },
                                            "Property": "Facility_Standard_Name",
                                        },
                                        "Name": "semantic vw_FacilityStandard.Facility_Standard_Name",
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "s3"}
                                            },
                                            "Property": "Process_Category_Code",
                                        },
                                        "Name": "semantic vw_Process.Process_Category_Code",
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "s3"}
                                            },
                                            "Property": "Process_Category_Description",
                                        },
                                        "Name": "semantic vw_Process.Process_Category_Description",
                                    },
                                    {
                                        "Aggregation": {
                                            "Expression": {
                                                "Column": {
                                                    "Expression": {
                                                        "SourceRef": {"Source": "s"}
                                                    },
                                                    "Property": "CB_Code",
                                                }
                                            },
                                            "Function": 3,
                                        },
                                        "Name": "Min(semantic vw_ScopeCertificate_SuppyChainOperator.CB_Code)",
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "s1"}
                                            },
                                            "Property": "TE ID",
                                        },
                                        "Name": "semantic vw_facility.TE ID",
                                        "NativeReferenceName": "TE ID",
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "s1"}
                                            },
                                            "Property": "Retired TE ID",
                                        },
                                        "Name": "semantic vw_facility.Retired TE ID",
                                        "NativeReferenceName": "Retired TE ID",
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "s1"}
                                            },
                                            "Property": "Facility Name",
                                        },
                                        "Name": "semantic vw_facility.Facility Name",
                                        "NativeReferenceName": "Name",
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "s1"}
                                            },
                                            "Property": "Facility Address",
                                        },
                                        "Name": "semantic vw_facility.Facility Address",
                                        "NativeReferenceName": "Address",
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "s1"}
                                            },
                                            "Property": "Facility Country",
                                        },
                                        "Name": "semantic vw_facility.Facility Country",
                                        "NativeReferenceName": "Country/Area",
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "s1"}
                                            },
                                            "Property": "Facility State",
                                        },
                                        "Name": "semantic vw_facility.Facility State",
                                        "NativeReferenceName": "State/Province",
                                    },
                                ],
                        
                                "Where": [
                                    {
                                        "Condition": {
                                            "In": {
                                                "Expressions": [
                                                    {
                                                        "Column": {
                                                            "Expression": {
                                                                "SourceRef": {
                                                                    "Source": "s1"
                                                                }
                                                            },
                                                            "Property": "facility_type",
                                                        }
                                                    }
                                                ],
                                                "Values": [
                                                    [
                                                        {
                                                            "Literal": {
                                                                "Value": "'Main Facility'"
                                                            }
                                                        }
                                                    ],
                                                    [
                                                        {
                                                            "Literal": {
                                                                "Value": "'Subsequent'"
                                                            }
                                                        }
                                                    ],
                                                ],
                                            }
                                        }
                                    },
                        
                                    {
                                        "Condition": {
                                            "In": {
                                                "Expressions": [
                                                    {
                                                        "Column": {
                                                            "Expression": {
                                                                "SourceRef": {
                                                                    "Source": "s"
                                                                }
                                                            },
                                                            "Property": "SCO_Name",
                                                        }
                                                    }
                                                ],
                                                "Values": [
                                                    [
                                                        {
                                                            "Literal": {
                                                                "Value": lit(
                                                                    row[
                                                                        "Certified Organization Name"].replace("'", "''")
                                                                    
                                                                )
                                                            }
                                                        }
                                                    ]
                                                ],
                                            }
                                        }
                                    },
                               
                                    {
                                        "Condition": {
                                            "In": {
                                                "Expressions": [
                                                    {
                                                        "Column": {
                                                            "Expression": {
                                                                "SourceRef": {
                                                                    "Source": "s"
                                                                }
                                                            },
                                                            "Property": "SCO_Country",
                                                        }
                                                    }
                                                ],
                                                "Values": [
                                                    [
                                                        {
                                                            "Literal": {
                                                                "Value": lit(
                                                                    row["Country/Area"]
                                                                )
                                                            }
                                                        }
                                                    ]
                                                ],
                                            }
                                        }
                                    },
                                 
                                    {
                                        "Condition": {
                                            "In": {
                                                "Expressions": [
                                                    {
                                                        "Column": {
                                                            "Expression": {
                                                                "SourceRef": {
                                                                    "Source": "s"
                                                                }
                                                            },
                                                            "Property": "SCO_License",
                                                        }
                                                    }
                                                ],
                                                "Values": [
                                                    [
                                                        {
                                                            "Literal": {
                                                                "Value": lit(
                                                                    row[
                                                                        "License Number"
                                                                    ]
                                                                )
                                                            }
                                                        }
                                                    ]
                                                ],
                                            }
                                        }
                                    },
                                 
                                    {
                                        "Condition": {
                                            "In": {
                                                "Expressions": [
                                                    {
                                                        "Column": {
                                                            "Expression": {
                                                                "SourceRef": {
                                                                    "Source": "s"
                                                                }
                                                            },
                                                            "Property": "SCO_Native_Name",
                                                        }
                                                    }
                                                ],
                                                "Values": [
                                                    [
                                                        {
                                                            "Literal": {
                                                                "Value": lit(
                                                                    row["Native Name"]
                                                                )
                                                            }
                                                        }
                                                    ]
                                                ],
                                            }
                                        }
                                    },
                               
                                    {
                                        "Condition": {
                                            "In": {
                                                "Expressions": [
                                                    {
                                                        "Column": {
                                                            "Expression": {
                                                                "SourceRef": {
                                                                    "Source": "s"
                                                                }
                                                            },
                                                            "Property": "SC_Number",
                                                        }
                                                    }
                                                ],
                                                "Values": [
                                                    [
                                                        {
                                                            "Literal": {
                                                                "Value": lit(
                                                                    row["SC Number"]
                                                                )
                                                            }
                                                        }
                                                    ]
                                                ],
                                            }
                                        }
                                    },
                        
                                    {
                                        "Condition": {
                                            "Contains": {
                                                "Left": {
                                                    "Column": {
                                                        "Expression": {
                                                            "SourceRef": {"Source": "s"}
                                                        },
                                                        "Property": "SC Number | SC Version No",
                                                    }
                                                },
                                                "Right": {
                                                    "Literal": {
                                                        "Value": lit(row["SC Number"])
                                                    }
                                                },
                                            }
                                        }
                                    },
                      
                                    {
                                        "Condition": {
                                            "In": {
                                                "Expressions": [
                                                    {
                                                        "Column": {
                                                            "Expression": {
                                                                "SourceRef": {
                                                                    "Source": "s"
                                                                }
                                                            },
                                                            "Property": "SC_Status",
                                                        }
                                                    }
                                                ],
                                                "Values": [
                                                    [
                                                        {
                                                            "Literal": {
                                                                "Value": lit(
                                                                    row["SC Status"]
                                                                )
                                                            }
                                                        }
                                                    ]
                                                ],
                                            }
                                        }
                                    },
                        
                                    {
                                        "Condition": {
                                            "In": {
                                                "Expressions": [
                                                    {
                                                        "Column": {
                                                            "Expression": {
                                                                "SourceRef": {
                                                                    "Source": "s"
                                                                }
                                                            },
                                                            "Property": "CB_Name",
                                                        }
                                                    },
                                                ],
                                                "Values": [
                                                    [
                                                        {
                                                            "Literal": {
                                                                "Value": lit(
                                                                    row[
                                                                        "Certification Body"
                                                                    ]
                                                                )
                                                            }
                                                        }
                                                    ]
                                                ],
                                            }
                                        }
                                    },
                      
                                    {
                                        "Condition": {
                                            "In": {
                                                "Expressions": [
                                                    {
                                                        "Column": {
                                                            "Expression": {
                                                                "SourceRef": {
                                                                    "Source": "s"
                                                                }
                                                            },
                                                            "Property": "CO_Te_Id",
                                                        }
                                                    }
                                                ],
                                                "Values": [
                                                    [
                                                        {
                                                            "Literal": {
                                                                "Value": lit(
                                                                    row["TE ID"]
                                                                )
                                                            }
                                                        }
                                                    ]
                                                ],
                                            }
                                        }
                                    },

                                    {
                                        "Condition": {
                                            "In": {
                                                "Expressions": [
                                                    {
                                                        "Column": {
                                                            "Expression": {
                                                                "SourceRef": {
                                                                    "Source": "s"
                                                                }
                                                            },
                                                            "Property": "Retired_CO_Te_Id",
                                                        }
                                                    }
                                                ],
                                                "Values": [
                                                    [
                                                        {
                                                            "Literal": {
                                                                "Value": lit(
                                                                    row["Retired TE ID"]
                                                                )
                                                            }
                                                        }
                                                    ]
                                                ],
                                            }
                                        }
                                    },
                                    {
                                        "Condition": {
                                            "In": {
                                                "Expressions": [
                                                    {
                                                        "Column": {
                                                            "Expression": {
                                                                "SourceRef": {
                                                                    "Source": "s"
                                                                }
                                                            },
                                                            "Property": "SC_Version_No",
                                                        }
                                                    }
                                                ],
                                                "Values": [
                                                    [
                                                        {
                                                            "Literal": {
                                                                "Value": lit(
                                                                    row[
                                                                        "SC Version Number"
                                                                    ]
                                                                )
                                                            }
                                                        }
                                                    ]
                                                ],
                                            }
                                        }
                                    },
                                    {
                                        "Condition": {
                                            "In": {
                                                "Expressions": [
                                                    {
                                                        "Column": {
                                                            "Expression": {
                                                                "SourceRef": {
                                                                    "Source": "s21"
                                                                }
                                                            },
                                                            "Property": "Grouped SC Standard",
                                                        }
                                                    }
                                                ],
                                                "Values": [
                                                    [
                                                        {
                                                            "Literal": {
                                                                "Value": lit(
                                                                    row["Standard"]
                                                                )
                                                            }
                                                        }
                                                    ]
                                                ],
                                            }
                                        }
                                    },
                                    {
                                        "Condition": {
                                            "In": {
                                                "Expressions": [
                                                    {
                                                        "Column": {
                                                            "Expression": {
                                                                "SourceRef": {
                                                                    "Source": "s"
                                                                }
                                                            },
                                                            "Property": "SC_Last_Updated_Date",
                                                        }
                                                    }
                                                ],
                                                "Values": [
                                                    [
                                                        {
                                                            "Literal": {
                                                                "Value": lit_date(
                                                                    row[
                                                                        "LastUpdated_Timestamp"
                                                                    ]
                                                                )
                                                            }
                                                        }
                                                    ]
                                                ],
                                            }
                                        }
                                    },
                                    {
                                        "Condition": {
                                            "And": {
                                                "Left": {
                                                    "Not": {
                                                        "Expression": {
                                                            "Comparison": {
                                                                "ComparisonKind": 0,
                                                                "Left": {
                                                                    "Column": {
                                                                        "Expression": {
                                                                            "SourceRef": {
                                                                                "Source": "s"
                                                                            }
                                                                        },
                                                                        "Property": "SC_Number",
                                                                    }
                                                                },
                                                                "Right": {
                                                                    "Literal": {
                                                                        "Value": "'CU1163654MUL-2023-00034573'"
                                                                    }
                                                                },
                                                            }
                                                        }
                                                    }
                                                },
                                                "Right": {
                                                    "Not": {
                                                        "Expression": {
                                                            "Comparison": {
                                                                "ComparisonKind": 0,
                                                                "Left": {
                                                                    "Column": {
                                                                        "Expression": {
                                                                            "SourceRef": {
                                                                                "Source": "s"
                                                                            }
                                                                        },
                                                                        "Property": "SC_Number",
                                                                    }
                                                                },
                                                                "Right": {
                                                                    "Literal": {
                                                                        "Value": "'CU807457MUL-2024-00115951'"
                                                                    }
                                                                },
                                                            }
                                                        }
                                                    }
                                                },
                                            }
                                        }
                                    },
                                    {
                                        "Condition": {
                                            "Not": {
                                                "Expression": {
                                                    "Comparison": {
                                                        "ComparisonKind": 0,
                                                        "Left": {
                                                            "Column": {
                                                                "Expression": {
                                                                    "SourceRef": {
                                                                        "Source": "s"
                                                                    }
                                                                },
                                                                "Property": "CB_Code",
                                                            }
                                                        },
                                                        "Right": {
                                                            "Literal": {
                                                                "Value": "'CB-DCB'"
                                                            }
                                                        },
                                                    }
                                                }
                                            }
                                        }
                                    },
                                    {
                                        "Condition": {
                                            "And": {
                                                "Left": {
                                                    "Not": {
                                                        "Expression": {
                                                            "Comparison": {
                                                                "ComparisonKind": 0,
                                                                "Left": {
                                                                    "Column": {
                                                                        "Expression": {
                                                                            "SourceRef": {
                                                                                "Source": "s"
                                                                            }
                                                                        },
                                                                        "Property": "SC_Number",
                                                                    }
                                                                },
                                                                "Right": {
                                                                    "Literal": {
                                                                        "Value": "'CU807457RAF-2024-000801115'"
                                                                    }
                                                                },
                                                            }
                                                        }
                                                    }
                                                },
                                                "Right": {
                                                    "Not": {
                                                        "Expression": {
                                                            "Comparison": {
                                                                "ComparisonKind": 0,
                                                                "Left": {
                                                                    "Column": {
                                                                        "Expression": {
                                                                            "SourceRef": {
                                                                                "Source": "s"
                                                                            }
                                                                        },
                                                                        "Property": "SC_Number",
                                                                    }
                                                                },
                                                                "Right": {
                                                                    "Literal": {
                                                                        "Value": "'CU1042889MUL-2024-00027504'"
                                                                    }
                                                                },
                                                            }
                                                        }
                                                    }
                                                },
                                            }
                                        }
                                    },
                                ],  #
                            },
                            "Binding": {
                                "Primary": {
                                    "Groupings": [
                                        {"Projections": [0]},
                                        {"Projections": [7]},
                                        {"Projections": [5]},
                                        {"Projections": [6]},
                                        {"Projections": [8]},
                                        {"Projections": [10]},
                                        {"Projections": [9]},
                                        {"Projections": [1]},
                                        {"Projections": [2]},
                                        {"Projections": [3, 4]},
                                    ]
                                },
                                "DataReduction": {
                                    "DataVolume": 3,
                                    "Primary": {"Window": {"Count": 500}},
                                },
                                "Version": 1,
                            },
                        }
                    }
                ]
            },
        }
    ],
}

# request
print("\n📡 Fetching FACILITY DATA...")


res_fac = post_with_auto_token(URL, headers=BASE_HEADERS, json=payload_Facility)


if res_fac.status_code != 200:
    print("❌ ERROR FACILITY:", res_fac.text)
    exit()

js_fac = res_fac.json()


# ==========================================================
# DECODE Facility Data
# ==========================================================
def extract_facilities(js):

    ds = js["results"][0]["result"]["data"]["dsr"]["DS"][0]
    vd = ds["ValueDicts"]
    PH = ds["PH"][0]

    def vdict(name, idx):
        try:
            return vd[name][idx]
        except:
            return None

    rows = []

    last = {
        "facility_type": None,
        "facility_name": None,
        "te_id": None,
        "retired_te_id": None,
        "address": None,
        "state": None,
        "country": None,
        "standard": None,
        "process_category_code": None,
        "process_category_description": None,
        "cb_code": None,
    }

    gmap = {
        "G0": ("facility_type", None),
        "G1": ("facility_name", "D0"),
        "G2": ("te_id", "D1"),
        "G3": ("retired_te_id", "D2"),
        "G4": ("address", "D3"),
        "G5": ("state", "D4"),
        "G6": ("country", "D5"),
        "G7": ("standard", "D6"),
        "G8": ("process_category_code", "D7"),
    }

    def walk(node, ctx):
        nonlocal last
        ctx = ctx.copy()

        
        for gx, (field, dict_name) in gmap.items():
            if gx in node:
                raw = node[gx]
                if isinstance(raw, str) or dict_name is None:
                    val = raw
                else:
                    val = vdict(dict_name, raw)
                ctx[field] = val
                last[field] = val

        
        if node.get("R") == 1:
            ctx.update(last)


        if "C" in node:
            pr = last.get("process_category_code")
            C = node["C"]

            if pr:


                if len(C) == 1:
                    desc = vdict("D8", C[0])
                    cb   = vdict("D9", 0) 
                    ctx["process_category_description"] = desc
                    ctx["cb_code"] = cb
                    rows.append(ctx.copy())


                elif len(C) == 2:
                    desc = vdict("D8", C[0])
                    cb   = vdict("D9", C[1])
                    ctx["process_category_description"] = desc
                    ctx["cb_code"] = cb
                    rows.append(ctx.copy())


        if "M" not in node:
            return

        for block in node["M"]:
            for dmname, children in block.items():
                for child in children:
                    walk(child, ctx)


    for root in PH["DM0"]:
        walk(root, {})

    return rows


# EXTRACTION DES FACILITIES
rows = extract_facilities(js_fac)

# DATAFRAME
df_fac = pd.DataFrame(rows)

df_fac = df_fac.rename(
    columns={
        "facility_type": "Facility Type",
        "facility_name": "Facility Name",
        "te_id": "TE ID",
        "retired_te_id": "Retired TE ID",
        "address": "Address",
        "state": "State/Province",
        "country": "Country/Area",
        "standard": "Standard",
        "process_category_code": "Process Category Code",
        "process_category_description": "Process Category Description",
        "cb_code": "CB Code",
    }
)

expected_cols = [
    "Facility Type",
    "Facility Name",
    "TE ID",
    "Retired TE ID",
    "Address",
    "State/Province",
    "Country/Area",
    "Standard",
    "Process Category Code",
    "Process Category Description",
    "CB Code",
]

for col in expected_cols:
    if col not in df_fac.columns:
        df_fac[col] = None

df_fac = df_fac[expected_cols]
print("✅ Facility DATA EXTRACTED SUCCESSFULLY")


row_facilities = df_fac.iloc[0]

def lit(v):
    if v is None or pd.isna(v) or v == "null":
        return "null"

    if isinstance(v, str) and v.strip() == "":
        return "''"

    return f"'{v}'"


# -----------------------------
# CONTACT
# -----------------------------
payload_contact = {
    "version": "1.0.0",
    "allowLongRunningQueries": True,
    "modelId": 4668616,
    "queries": [
        {
            "QueryId": "",
            "ApplicationContext": {
                "DatasetId": "30385b55-d526-401d-8379-6a502d67c719",
                "Sources": [
                    {
                        "ReportId": "5bbb73cf-c2c7-4c8d-8461-f04b75a9438b",
                        "VisualId": "269b05deaa4bee5d72dd",
                    }
                ],
            },
            "Query": {
                "Commands": [
                    {
                        "SemanticQueryDataShapeCommand": {
                            "Query": {
                                "Version": 2,
                                "From": [
                                    {
                                        "Name": "s1",
                                        "Entity": "semantic vw_supply_chain_operator_email",
                                        "Type": 0,
                                    },
                                    {
                                        "Name": "s2",
                                        "Entity": "semantic vw_ScopeCertificate_SuppyChainOperator",
                                        "Type": 0,
                                    },
                                    {
                                        "Name": "s11",
                                        "Entity": "semantic vw_facility",
                                        "Type": 0,
                                    },
                                    {
                                        "Name": "s21",
                                        "Entity": "semantic vw_ScopeCertificateStandard",
                                        "Type": 0,
                                    },
                                ],
                                "Select": [
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "s1"}
                                            },
                                            "Property": "SCO_Public_Email",
                                        },
                                        "Name": "semantic vw_supply_chain_operator_email.SCO_Public_Email",
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "s2"}
                                            },
                                            "Property": "SCO_Contact",
                                        },
                                        "Name": "semantic vw_ScopeCertificate_SuppyChainOperator.SCO_Contact",
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "s2"}
                                            },
                                            "Property": "SCO_Address",
                                        },
                                        "Name": "semantic vw_ScopeCertificate_SuppyChainOperator.SCO_Address",
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "s2"}
                                            },
                                            "Property": "SCO_State",
                                        },
                                        "Name": "semantic vw_ScopeCertificate_SuppyChainOperator.SCO_State",
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "s2"}
                                            },
                                            "Property": "SCO_Country",
                                        },
                                        "Name": "semantic vw_ScopeCertificate_SuppyChainOperator.SCO_Country",
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "s2"}
                                            },
                                            "Property": "SCO_Website",
                                        },
                                        "Name": "semantic vw_ScopeCertificate_SuppyChainOperator.SCO_Website",
                                    },
                                ],
                                "Where": [
                                    {
                                        "Condition": {
                                            "In": {
                                                "Expressions": [
                                                    {
                                                        "Column": {
                                                            "Expression": {
                                                                "SourceRef": {
                                                                    "Source": "s2"
                                                                }
                                                            },
                                                            "Property": "SCO_Name",
                                                        }
                                                    }
                                                ],
                                                "Values": [
                                                    [
                                                        {
                                                            "Literal": {
                                                                "Value": lit(
                                                                    row[
                                                                        "Certified Organization Name"].replace("'", "''")
                                                                
                                                                )
                                                            }
                                                        }
                                                    ]
                                                ],
                                            }
                                        }
                                    },
                                    {
                                        "Condition": {
                                            "In": {
                                                "Expressions": [
                                                    {
                                                        "Column": {
                                                            "Expression": {
                                                                "SourceRef": {
                                                                    "Source": "s2"
                                                                }
                                                            },
                                                            "Property": "SCO_Country",
                                                        }
                                                    }
                                                ],
                                                "Values": [
                                                    [
                                                        {
                                                            "Literal": {
                                                                "Value": lit(
                                                                    row["Country/Area"]
                                                                )
                                                            }
                                                        }
                                                    ]
                                                ],
                                            }
                                        }
                                    },
                                    {
                                        "Condition": {
                                            "In": {
                                                "Expressions": [
                                                    {
                                                        "Column": {
                                                            "Expression": {
                                                                "SourceRef": {
                                                                    "Source": "s2"
                                                                }
                                                            },
                                                            "Property": "SCO_License",
                                                        }
                                                    }
                                                ],
                                                "Values": [
                                                    [
                                                        {
                                                            "Literal": {
                                                                "Value": lit(
                                                                    row[
                                                                        "License Number"
                                                                    ]
                                                                )
                                                            }
                                                        }
                                                    ]
                                                ],
                                            }
                                        }
                                    },
                                    {
                                        "Condition": {
                                            "In": {
                                                "Expressions": [
                                                    {
                                                        "Column": {
                                                            "Expression": {
                                                                "SourceRef": {
                                                                    "Source": "s2"
                                                                }
                                                            },
                                                            "Property": "SCO_Native_Name",
                                                        }
                                                    }
                                                ],
                                                "Values": [
                                                    [
                                                        {
                                                            "Literal": {
                                                                "Value": lit(
                                                                    row["Native Name"]
                                                                )
                                                            }
                                                        }
                                                    ]
                                                ],
                                            }
                                        }
                                    },
                                    {
                                        "Condition": {
                                            "In": {
                                                "Expressions": [
                                                    {
                                                        "Column": {
                                                            "Expression": {
                                                                "SourceRef": {
                                                                    "Source": "s2"
                                                                }
                                                            },
                                                            "Property": "SC_Number",
                                                        }
                                                    }
                                                ],
                                                "Values": [
                                                    [
                                                        {
                                                            "Literal": {
                                                                "Value": lit(
                                                                    row["SC Number"]
                                                                )
                                                            }
                                                        }
                                                    ]
                                                ],
                                            }
                                        }
                                    },
                                    {
                                        "Condition": {
                                            "In": {
                                                "Expressions": [
                                                    {
                                                        "Column": {
                                                            "Expression": {
                                                                "SourceRef": {
                                                                    "Source": "s2"
                                                                }
                                                            },
                                                            "Property": "SC_Status",
                                                        }
                                                    }
                                                ],
                                                "Values": [
                                                    [
                                                        {
                                                            "Literal": {
                                                                "Value": lit(
                                                                    row["SC Status"]
                                                                )
                                                            }
                                                        }
                                                    ]
                                                ],
                                            }
                                        }
                                    },
                                    {
                                        "Condition": {
                                            "In": {
                                                "Expressions": [
                                                    {
                                                        "Column": {
                                                            "Expression": {
                                                                "SourceRef": {
                                                                    "Source": "s2"
                                                                }
                                                            },
                                                            "Property": "Retired_CO_Te_Id",
                                                        }
                                                    }
                                                ],
                                                "Values": [
                                                    [
                                                        {
                                                            "Literal": {
                                                                "Value": lit(
                                                                    row["Retired TE ID"]
                                                                )
                                                            }
                                                        }
                                                    ]
                                                ],
                                            }
                                        }
                                    },
                                    {
                                        "Condition": {
                                            "Contains": {
                                                "Left": {
                                                    "Column": {
                                                        "Expression": {
                                                            "SourceRef": {
                                                                "Source": "s11"
                                                            }
                                                        },
                                                        "Property": "Facility_Name",
                                                    }
                                                },
                                                "Right": {
                                                    "Literal": {
                                                        "Value": lit(
                                                            row_facilities[
                                                                "Facility Name"].replace("'", "''")
                                                            
                                                        )
                                                    }
                                                },
                                            }
                                        }
                                    },
                                    {
                                        "Condition": {
                                            "In": {
                                                "Expressions": [
                                                    {
                                                        "Column": {
                                                            "Expression": {
                                                                "SourceRef": {
                                                                    "Source": "s2"
                                                                }
                                                            },
                                                            "Property": "CB_Name",
                                                        }
                                                    }
                                                ],
                                                "Values": [
                                                    [
                                                        {
                                                            "Literal": {
                                                                "Value": lit(
                                                                    row[
                                                                        "Certification Body"
                                                                    ]
                                                                )
                                                            }
                                                        }
                                                    ]
                                                ],
                                            }
                                        }
                                    },
                                    {
                                        "Condition": {
                                            "In": {
                                                "Expressions": [
                                                    {
                                                        "Column": {
                                                            "Expression": {
                                                                "SourceRef": {
                                                                    "Source": "s2"
                                                                }
                                                            },
                                                            "Property": "CO_Te_Id",
                                                        }
                                                    }
                                                ],
                                                "Values": [
                                                    [
                                                        {
                                                            "Literal": {
                                                                "Value": lit(
                                                                    row["TE ID"]
                                                                )
                                                            }
                                                        }
                                                    ]
                                                ],
                                            }
                                        }
                                    },
                                    {
                                        "Condition": {
                                            "In": {
                                                "Expressions": [
                                                    {
                                                        "Column": {
                                                            "Expression": {
                                                                "SourceRef": {
                                                                    "Source": "s2"
                                                                }
                                                            },
                                                            "Property": "SC_Version_No",
                                                        }
                                                    }
                                                ],
                                                "Values": [
                                                    [
                                                        {
                                                            "Literal": {
                                                                "Value": lit(
                                                                    row[
                                                                        "SC Version Number"
                                                                    ]
                                                                )
                                                            }
                                                        }
                                                    ]
                                                ],
                                            }
                                        }
                                    },
                                    {
                                        "Condition": {
                                            "In": {
                                                "Expressions": [
                                                    {
                                                        "Column": {
                                                            "Expression": {
                                                                "SourceRef": {
                                                                    "Source": "s21"
                                                                }
                                                            },
                                                            "Property": "Grouped SC Standard",
                                                        }
                                                    }
                                                ],
                                                "Values": [
                                                    [
                                                        {
                                                            "Literal": {
                                                                "Value": lit(
                                                                    row["Standard"]
                                                                )
                                                            }
                                                        }
                                                    ]
                                                ],
                                            }
                                        }
                                    },
                                    {
                                        "Condition": {
                                            "In": {
                                                "Expressions": [
                                                    {
                                                        "Column": {
                                                            "Expression": {
                                                                "SourceRef": {
                                                                    "Source": "s2"
                                                                }
                                                            },
                                                            "Property": "SC_Last_Updated_Date",
                                                        }
                                                    }
                                                ],
                                                "Values": [
                                                    [
                                                        {
                                                            "Literal": {
                                                                "Value": lit(
                                                                    row[
                                                                        "LastUpdated_Timestamp"
                                                                    ]
                                                                )
                                                            }
                                                        }
                                                    ]
                                                ],
                                            }
                                        }
                                    },
                                ],
                            },
                            "Binding": {
                                "Primary": {
                                    "Groupings": [
                                        {
                                            "Projections": [1, 2, 3, 4, 0, 5],
                                            "ShowItemsWithNoData": [1, 2, 3, 4, 0, 5],
                                        }
                                    ]
                                },
                                "DataReduction": {
                                    "DataVolume": 3,
                                    "Primary": {"Window": {"Count": 500}},
                                },
                                "Version": 1,
                            },
                        }
                    }
                ]
            },
        }
    ],
}


# REQUEST
print("\n📡 Fetching CONTACT DATA...")

res_contact = post_with_auto_token(URL, headers=BASE_HEADERS, json=payload_contact)

if res_contact.status_code != 200:
    print("❌ ERROR CONTACT:", res_contact.text)
    exit()

js_contact = res_contact.json()


# ==========================================
# DECODE
# ==========================================
def decode_contact(js):
    try:
        ds = js["results"][0]["result"]["data"]["dsr"]["DS"][0]
    except:
        return []

    vd = ds.get("ValueDicts", {})
    rows_raw = ds["PH"][0]["DM0"]

    rows = []

    for r in rows_raw:
        C = r["C"] 

        contact = vd["D0"][C[0]] if len(vd["D0"]) > 0 else None
        addr = vd["D1"][C[1]] if len(vd["D1"]) > 0 else None
        state = vd["D2"][C[2]] if len(vd["D2"]) > 0 else None
        country = vd["D3"][C[3]] if len(vd["D3"]) > 0 else None
        email = vd["D4"][C[4]] if len(vd["D4"]) > 0 else None
        website = vd["D5"][0] if len(vd["D5"]) > 0 else None

        rows.append([contact, addr, state, country, email, website])

    return rows



# DATAFRAME
df_contact = pd.DataFrame(
    decode_contact(js_contact),
    columns=[
        "Contact",
        "Adress",
        "State/Province",
        "Country/Area",
        "Public Email",
        "Website",
    ],
)
print("✅ CONTACT DATA EXTRACTED SUCCESSFULLY")


# -----------------------------
#  SC_Certificate
# -----------------------------

payload_certificate = {
    "version": "1.0.0",
    "allowLongRunningQueries": True,
    "modelId": 4668616,
    "queries": [
        {
            "QueryId": "",
            "ApplicationContext": {
                "DatasetId": "30385b55-d526-401d-8379-6a502d67c719",
                "Sources": [
                    {
                        "ReportId": "5bbb73cf-c2c7-4c8d-8461-f04b75a9438b",
                        "VisualId": "64d20318459ec7516171",
                    }
                ],
            },
            "Query": {
                "Commands": [
                    {
                        "SemanticQueryDataShapeCommand": {
                            "ExecutionMetricsKind": 1,
                            "Query": {
                                "Version": 2,
                                "From": [
                                    {
                                        "Name": "s2",
                                        "Entity": "semantic vw_ScopeCertificate_SuppyChainOperator",
                                        "Type": 0,
                                    },
                                    {
                                        "Name": "s3",
                                        "Entity": "semantic vw_ScopeCertificateStandard",
                                        "Type": 0,
                                    },
                                ],
                                "Select": [
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "s2"}
                                            },
                                            "Property": "SC_Number",
                                        }
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "s2"}
                                            },
                                            "Property": "SCO_Name",
                                        }
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "s3"}
                                            },
                                            "Property": "SC_Program",
                                        }
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "s2"}
                                            },
                                            "Property": "SC_IssuedDate",
                                        }
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "s2"}
                                            },
                                            "Property": "SC_ValidToDate",
                                        }
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "s2"}
                                            },
                                            "Property": "SC_Version_No",
                                        }
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "s2"}
                                            },
                                            "Property": "SC Grouped Standards",
                                        }
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "s2"}
                                            },
                                            "Property": "CO_Te_Id",
                                        },
                                      
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "s2"}
                                            },
                                            "Property": "Retired_CO_Te_Id",
                                        },
                                        "NativeReferenceName": "Retired TE ID",
                                    },
                                ],
                                "Where": [
                                    {
                                        "Condition": {
                                            "Comparison": {
                                                "ComparisonKind": 0,
                                                "Left": {
                                                    "Column": {
                                                        "Expression": {
                                                            "SourceRef": {
                                                                "Source": "s2"
                                                            }
                                                        },
                                                        "Property": "Record_Add_Flag",
                                                    }
                                                },
                                                "Right": {"Literal": {"Value": "0L"}},
                                            }
                                        }
                                    },
                                    {
                                        "Condition": {
                                            "In": {
                                                "Expressions": [
                                                    {
                                                        "Column": {
                                                            "Expression": {
                                                                "SourceRef": {
                                                                    "Source": "s2"
                                                                }
                                                            },
                                                            "Property": "SCO_Name",
                                                        }
                                                    }
                                                ],
                                                "Values": [
                                                    [
                                                        {
                                                            "Literal": {
                                                                "Value": lit(
                                                                    row[
                                                                        "Certified Organization Name"].replace("'", "''")
                                                                    
                                                                )
                                                            }
                                                        }
                                                    ]
                                                ],
                                            }
                                        }
                                    },
                                    {
                                        "Condition": {
                                            "In": {
                                                "Expressions": [
                                                    {
                                                        "Column": {
                                                            "Expression": {
                                                                "SourceRef": {
                                                                    "Source": "s2"
                                                                }
                                                            },
                                                            "Property": "SCO_Country",
                                                        }
                                                    }
                                                ],
                                                "Values": [
                                                    [
                                                        {
                                                            "Literal": {
                                                                "Value": lit(
                                                                    row["Country/Area"]
                                                                )
                                                            }
                                                        }
                                                    ]
                                                ],
                                            }
                                        }
                                    },
                                    {
                                        "Condition": {
                                            "In": {
                                                "Expressions": [
                                                    {
                                                        "Column": {
                                                            "Expression": {
                                                                "SourceRef": {
                                                                    "Source": "s2"
                                                                }
                                                            },
                                                            "Property": "SCO_License",
                                                        }
                                                    }
                                                ],
                                                "Values": [
                                                    [
                                                        {
                                                            "Literal": {
                                                                "Value": lit(
                                                                    row[
                                                                        "License Number"
                                                                    ]
                                                                )
                                                            }
                                                        }
                                                    ]
                                                ],
                                            }
                                        }
                                    },
                                    {
                                        "Condition": {
                                            "In": {
                                                "Expressions": [
                                                    {
                                                        "Column": {
                                                            "Expression": {
                                                                "SourceRef": {
                                                                    "Source": "s2"
                                                                }
                                                            },
                                                            "Property": "SCO_Native_Name",
                                                        }
                                                    }
                                                ],
                                                "Values": [
                                                    [
                                                        {
                                                            "Literal": {
                                                                "Value": lit(
                                                                    row["Native Name"]
                                                                )
                                                            }
                                                        }
                                                    ]
                                                ],
                                            }
                                        }
                                    },
                                    {
                                        "Condition": {
                                            "In": {
                                                "Expressions": [
                                                    {
                                                        "Column": {
                                                            "Expression": {
                                                                "SourceRef": {
                                                                    "Source": "s2"
                                                                }
                                                            },
                                                            "Property": "SC_Number",
                                                        }
                                                    }
                                                ],
                                                "Values": [
                                                    [
                                                        {
                                                            "Literal": {
                                                                "Value": lit(
                                                                    row["SC Number"]
                                                                )
                                                            }
                                                        }
                                                    ]
                                                ],
                                            }
                                        }
                                    },
                                    {
                                        "Condition": {
                                            "In": {
                                                "Expressions": [
                                                    {
                                                        "Column": {
                                                            "Expression": {
                                                                "SourceRef": {
                                                                    "Source": "s2"
                                                                }
                                                            },
                                                            "Property": "SC_Status",
                                                        }
                                                    }
                                                ],
                                                "Values": [
                                                    [
                                                        {
                                                            "Literal": {
                                                                "Value": lit(
                                                                    row["SC Status"]
                                                                )
                                                            }
                                                        }
                                                    ]
                                                ],
                                            }
                                        }
                                    },
                                    {
                                        "Condition": {
                                            "In": {
                                                "Expressions": [
                                                    {
                                                        "Column": {
                                                            "Expression": {
                                                                "SourceRef": {
                                                                    "Source": "s2"
                                                                }
                                                            },
                                                            "Property": "CB_Name",
                                                        }
                                                    }
                                                ],
                                                "Values": [
                                                    [
                                                        {
                                                            "Literal": {
                                                                "Value": lit(
                                                                    row[
                                                                        "Certification Body"
                                                                    ]
                                                                )
                                                            }
                                                        }
                                                    ]
                                                ],
                                            }
                                        }
                                    },
                                    {
                                        "Condition": {
                                            "In": {
                                                "Expressions": [
                                                    {
                                                        "Column": {
                                                            "Expression": {
                                                                "SourceRef": {
                                                                    "Source": "s2"
                                                                }
                                                            },
                                                            "Property": "CO_Te_Id",
                                                        }
                                                    }
                                                ],
                                                "Values": [
                                                    [
                                                        {
                                                            "Literal": {
                                                                "Value": lit(
                                                                    row["TE ID"]
                                                                )
                                                            }
                                                        }
                                                    ]
                                                ],
                                            }
                                        }
                                    },
                                    {
                                        "Condition": {
                                            "In": {
                                                "Expressions": [
                                                    {
                                                        "Column": {
                                                            "Expression": {
                                                                "SourceRef": {
                                                                    "Source": "s2"
                                                                }
                                                            },
                                                            "Property": "Retired_CO_Te_Id",
                                                        }
                                                    }
                                                ],
                                                "Values": [
                                                    [
                                                        {
                                                            "Literal": {
                                                                "Value": lit(
                                                                    row["Retired TE ID"]
                                                                )
                                                            }
                                                        }
                                                    ]
                                                ],
                                            }
                                        }
                                    },
                                    {
                                        "Condition": {
                                            "In": {
                                                "Expressions": [
                                                    {
                                                        "Column": {
                                                            "Expression": {
                                                                "SourceRef": {
                                                                    "Source": "s2"
                                                                }
                                                            },
                                                            "Property": "SC_Version_No",
                                                        }
                                                    }
                                                ],
                                                "Values": [
                                                    [
                                                        {
                                                            "Literal": {
                                                                "Value": lit(
                                                                    row[
                                                                        "SC Version Number"
                                                                    ]
                                                                )
                                                            }
                                                        }
                                                    ]
                                                ],
                                            }
                                        }
                                    },
                                    {
                                        "Condition": {
                                            "In": {
                                                "Expressions": [
                                                    {
                                                        "Column": {
                                                            "Expression": {
                                                                "SourceRef": {
                                                                    "Source": "s3"
                                                                }
                                                            },
                                                            "Property": "Grouped SC Standard",
                                                        }
                                                    }
                                                ],
                                                "Values": [
                                                    [
                                                        {
                                                            "Literal": {
                                                                "Value": lit(
                                                                    row["Standard"]
                                                                )
                                                            }
                                                        }
                                                    ]
                                                ],
                                            }
                                        }
                                    },
                                    {
                                        "Condition": {
                                            "Contains": {
                                                "Left": {
                                                    "Column": {
                                                        "Expression": {
                                                            "SourceRef": {
                                                                "Source": "s2"
                                                            }
                                                        },
                                                        "Property": "SC Number | SC Version No",
                                                    }
                                                },
                                                "Right": {
                                                    "Literal": {
                                                        "Value": lit(row["SC Number"])
                                                    }
                                                },
                                            }
                                        }
                                    },
                                ],
                            },
                            "Binding": {
                                "Primary": {
                                    "Groupings": [
                                        {
                                            "Projections": [0, 5, 7, 8, 1, 3, 4, 2, 6],
                                            "ShowItemsWithNoData": [
                                                0,
                                                5,
                                                7,
                                                8,
                                                1,
                                                3,
                                                4,
                                                2,
                                                6,
                                            ],
                                        }
                                    ]
                                },
                                "DataReduction": {
                                    "DataVolume": 3,
                                    "Primary": {"Window": {"Count": 500}},
                                },
                                "Version": 1,
                            },
                        }
                    }
                ]
            },
        }
    ],
}


print("\n📡 Fetching SCOPE CERTIFICATE DATA...")

res_certificate = post_with_auto_token(URL, headers=BASE_HEADERS, json=payload_certificate)

if res_certificate.status_code != 200:
    print("❌ ERROR CERTIFICATE:", res_certificate.text)
    exit()

js_certificate = res_certificate.json()



# ==========================================================
# DCODEUR
# ==========================================================
from datetime import datetime, timezone

def extract_sc(js):
    data = js["results"][0]["result"]["data"]["dsr"]["DS"][0]

    vd = data["ValueDicts"]
    dm0 = data["PH"][0]["DM0"][0]
    C = dm0["C"]

    DATE_INDEX = {
        "G5": 4,  
        "G6": 5,  
    }

    def val(key):
        for entry in dm0["S"]:
            if entry["N"] == key:


                if "DN" in entry:
                    dn = entry["DN"]
                    idx = dm0.get(key)

                    if dn not in vd or not vd[dn]:
                        return None

                    if isinstance(idx, int) and idx < len(vd[dn]):
                        return vd[dn][idx]

                    return vd[dn][0]

                if entry.get("T") == 7 and key in DATE_INDEX:
                    ts = C[DATE_INDEX[key]]
                    return (
                        datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
                        .strftime("%Y-%m-%d")
                        if ts else None
                    )

        return None
    
    return {
        "SC_Number": val("G0"),
        "SC_Version_No": val("G1"),
        "CO_Te_Id": val("G2"),
        "Retired_CO_Te_Id": val("G3"),
        "SCO_Name": val("G4"),
        "SC_IssuedDate": val("G5"),
        "SC_ValidToDate": val("G6"),
        "SC_Program": val("G7"),
        "SC_Grouped_Standards": val("G8"),
    }


result = extract_sc(js_certificate)
df_scope_certificate = pd.DataFrame([result])

print("✅ SCOPE CERTIFICATE DATA EXTRACTED SUCCESSFULLY")


def df_to_html_rows(df):
    if df is None or df.empty:
        return ""

    rows_html = []

    for _, row in df.iterrows():
        tds = []
        for value in row:
            if value is None or pd.isna(value):
                tds.append("<td></td>")
            else:
                tds.append(f"<td>{value}</td>")
        rows_html.append("<tr>" + "".join(tds) + "</tr>")

    return "\n".join(rows_html)

Site_Listing_ROWS= df_to_html_rows(df_sc)
CONTACT_ROWS = df_to_html_rows(df_contact)
SC_ROWS = df_to_html_rows(df_scope_certificate)      # Scope Certificate Data
FACILITY_ROWS = df_to_html_rows(df_fac)
SCOPE_PRODUCT_ROWS = df_to_html_rows(df_products)

HTML_TEMPLATE_PATH = "/Users/mac/generateToken/TE_HTML.html"

with open(HTML_TEMPLATE_PATH, "r", encoding="utf-8") as f:
    html = f.read()
html = html.replace("{{Site_Listing_ROWS}}", Site_Listing_ROWS)
html = html.replace("{{CONTACT_ROWS}}", CONTACT_ROWS)
html = html.replace("{{SC_ROWS}}", SC_ROWS)
html = html.replace("{{FACILITY_ROWS}}", FACILITY_ROWS)
html = html.replace("{{SCOPE_PRODUCT_ROWS}}", SCOPE_PRODUCT_ROWS)
today = datetime.today().strftime("%d %B %Y")
html = html.replace("{{LAST_REFRESHED}}", today)

OUTPUT_HTML = f"TE_Scope_Certificate_{sc_number}.html"

with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
    f.write(html)

print(f"✅ HTML GENERATED → {OUTPUT_HTML}")

OUTPUT_PNG = f"TE_Scope_Certificate_{sc_number}.png"

asyncio.run(
    screenshot_html(OUTPUT_HTML, OUTPUT_PNG)
)

print(f"📸 SCREENSHOT GENERATED → {OUTPUT_PNG}")

