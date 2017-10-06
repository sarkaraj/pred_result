START_DATE_ORDER = '2017-09-14'  # # Date is INCLUSIVE
END_DATE_ORDER = '2017-09-19'  # # Date is EXCLUSIVE

VISIT_LIST_LOCATION = "wasb://skuopt@conapocv2standardsa.blob.core.windows.net/AZ_TCAS_VL.csv"
AGLU_LIST_LOCATION = "wasb://skuopt@conapocv2standardsa.blob.core.windows.net/AZ_TCAS_AGLU.csv"

FINAL_PREDICTION_LOCATION = "/CONA_CSO/final_predicted"

conversion_denominator = {'I': 7, 'II': 7, 'III': 7, 'VII': 7, 'IV': 31, 'V': 31, 'VI': 31, 'VIII': 31, 'IX': 31,
                          'X': 31}
