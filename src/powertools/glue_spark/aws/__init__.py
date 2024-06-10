from .redshift import RedshiftUtils
from .s3 import S3Utils


class AWS:

    s3 = S3Utils()
    redshift = RedshiftUtils()