"""
Run ETL application
"""
import logging
import logging.config
import yaml


def main():
    """
    Entry point to run the ETL job
    """
    pass
    #Parsing YAML file
    config_path = 'C:/Users/dayang.tie/extra_project/Small_ETL_Project/configs/xetra_report1_config.yml'
    config = yaml.safe_load(open(config_path))
    #configure logging
    log_config = config['logging']
    logging.config.dictConfig(log_config)
    logger = logging.getLogger(__name__)

if __name__ == '__main__':
    main()