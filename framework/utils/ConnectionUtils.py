from framework.utils.ConfigUtils import ConfigUtils
from framework.utils.SecretUtils import SecretUtils as Sr
import logging as logs

py4j_logger = logs.getLogger("py4j").setLevel(logs.INFO)
Config = ConfigUtils().getConfig()
if Config.has_section('Audit'):
    if Config.has_option('Audit','Audit.log.level'):
        log_level = Config.get('Audit','Audit.log.level')
        logs.basicConfig()
        logs.getLogger().setLevel(getattr(logs,log_level))


class ConnectionUtils():
    @staticmethod
    def getConnection(source):
        Con = ConfigUtils().getConfig()
        env = Con.get('Environment', 'server.environment')
        region_name = Con.get('Environment', 'server.region.name')
        configProp = "secret." + str(source).lower() + ".name"
        if Con.has_section('Secret'):
            if Con.has_option('Secret', configProp):
                secret_name = Con.get('Secret', configProp)
            else:
                secret_name = ""
                logs.error(f"::::Source {source} secret name not found in Config.properties file.")
        else:
            secret_name = ""

        if secret_name.strip() != "":
            secret = Sr.getSecret(secret_name, region_name)
            if source.upper() == "REDSHIFT":
                username = secret['username']
                password = secret['password']
                host = secret['host']
                port = int(secret['port'])
                dbname = secret['dbname']
                driver = secret['driver'] if 'driver' in secret.keys() else None
                url = "jdbc:redshift://{}:{}/{}".format(host, str(port), dbname)
                Prop = dict(driver=driver, user=username, password=password)
            elif source.upper() == "RDS-POSTGRES":
                username = secret['username']
                password = secret['password']
                host = secret['host']
                port = int(secret['port'])
                dbname = secret['dbname']
                driver = secret['driver'] if 'driver' in secret.keys() else None
                url = "jdbc:postgresql://{}:{}/{}".format(host, str(port), dbname)
                Prop = dict(driver=driver, user=username, password=password)
            else:
                logs.warning("::::Connection is not defined for the Source {} in Environment {}".format(source, env))
                host = ""
                port = 0
                dbname = ""
                url = ""
                Prop = dict()
            return (url, Prop, host, port, dbname)
        else:
            logs.error("::::Secret Name missing in Config file for the Source {}".format(source))
            return ("", dict(), "", 0, "")