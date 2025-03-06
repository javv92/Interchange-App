

class Parameters:
    """Class for store parameters for visa files"""
    def __init__(self, *args):
        super(Parameters, self).__init__(*args)

    def getTCRParameters(self)-> dict:
        """returns tcr list for interpretation
        
        Returns:
            tcr_params (dict): dictionary with configuration.

        """
        tcr_params = {
            'TCR_LIST': ['TCR0','TCR1','TCR2','TCR3','TCR4','TCR5','TCR6','TCR7']
        }

        return tcr_params

    def getARDEFParameters(self)-> dict:
        """store ARDEF configuration.

        Returns:
            params (dict): Dictionary with configuration.
        """
        params = {
            "tables": {
                "ARDEF": {
                    "table_type": {"start": 0, "end": 2},
                    "table_mnemonic": {"start": 2, "end": 10},
                    "record_type": {"start": 10, "end": 11},
                    "table_key": {"start": 11, "end": 23},
                    "effective_date": {"start": 23, "end": 31},
                    "delete_indicator": {"start": 31, "end": 32},
                    "low_key_for_range": {"start": 32, "end": 44},
                    "issuer_identifier": {"start": 44, "end": 50},
                    "check_digit_algorithm": {"start": 50, "end": 51},
                    "account_number_length": {"start": 51, "end": 53},
                    "token_indicator": {"start": 53, "end": 54},
                    "reserved": {"start": 54, "end": 55},
                    "base_ii_cib": {"start": 55, "end": 61},
                    "domain": {"start": 61, "end": 62},
                    "region": {"start": 62, "end": 63},
                    "country": {"start": 63, "end": 65},
                    "large_ticket": {"start": 65, "end": 66},
                    "technology_indicator": {"start": 66, "end": 67},
                    "ardef_region": {"start": 67, "end": 68},
                    "ardef_country": {"start": 68, "end": 70},
                    "commercial_card_level_2_data_indicator": {"start": 70, "end": 71},
                    "commercial_card_level_3_enhanced_data_indicator": {
                        "start": 71,
                        "end": 72,
                    },
                    "commercial_card_pos_prompting_indicator": {"start": 72, "end": 73},
                    "commercial_card_electronic_vat_evidence_indicator": {
                        "start": 73,
                        "end": 74,
                    },
                    "original_credit": {"start": 74, "end": 75},
                    "account_level_processing_indicator": {"start": 75, "end": 76},
                    "original_credit_money_transfer": {"start": 76, "end": 77},
                    "original_credit_online_gambling": {"start": 77, "end": 78},
                    "product_id": {"start": 78, "end": 80},
                    "combo_card": {"start": 80, "end": 81},
                    "fast_funds": {"start": 81, "end": 82},
                    "travel_indicator": {"start": 82, "end": 83},
                    "b2b_program_id": {"start": 83, "end": 85},
                    "prepaid_program_indicator": {"start": 85, "end": 86},
                    "rrr123": {"start": 86, "end": 89},
                    "account_funding_source": {"start": 89, "end": 90},
                    "settlement_match": {"start": 90, "end": 91},
                    "travel_account_data": {"start": 91, "end": 92},
                    "account_restricted_use": {"start": 92, "end": 93},
                    "nnss_indicator": {"start": 93, "end": 94},
                    "product_subtype": {"start": 94, "end": 96},
                    "alternate_atm": {"start": 96, "end": 97},
                    "reserved_1": {"start": 97, "end": 98},
                    "reserved_2": {"start": 98, "end": 100},
                }
            }
        }
        return params
