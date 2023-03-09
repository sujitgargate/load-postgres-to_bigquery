from pyspark.sql.functions import col, udf
from pyspark.sql.types import FloatType
import calendar, time

class Custom_UDF_Class:
    def convertINRToUSD(INRCurrencyAMT):
        usd = (INRCurrencyAMT)/80.00
        return (usd)

    def convertCurrency_UDF_function(self, column_name):
        convertCurrencyUDF =  udf(lambda column_name  : Custom_UDF_obj.convertINRToUSD(column_name), FloatType())
        # spark.udf.register("convertCurrencyUDF", convertINRToUSD,FloatType())
        return convertCurrencyUDF

    def convert_timezone(self, timezone, date_column_name):
        if timezone == "GMT":
            calendar.timegm(time.strptime((date_column_name), '%Y-%m-%d %H:%M:%S'))
        pass

Custom_UDF_obj = Custom_UDF_Class()