import os

class getquery():
    """Class to store querys for ingest of iar table
    
    Params:
        schema (str): schema of source table.
        table_name (str): source table name.
        main_schema (str): main schema.
        main_table (str): main table name.
    
    """

    def __init__(self,schema,table_name,main_schema,main_table) -> None:
        self.schema = schema
        self.table_name = table_name
        self.main_schema = main_schema
        self.main_table = main_table
        pass
        
    def up_from_temp(self)->str:
        """Query to update principal table where efective date is lower than the new data, returns query as str."""
        return  f"""
                    create temporary table tmp_up_from_temp_{self.table_name} as (
                    select c.delete_indicator,c.low_key_for_range as "lowrange",c.table_key as "tablekey",
                    c.effective_date as "effectivedate", c.app_date_valid as "date_valid"
                    from {self.main_schema}.{self.main_table} b
                    left join {self.schema}.{self.table_name} c on b.low_key_for_range = c.low_key_for_range and b.table_key = c.table_key
                    and b.effective_date < c.effective_date and b.app_date_end is null);
                    update {self.main_schema}.{self.main_table} b set app_date_end = d.date_valid - INTERVAL '1 DAY'
                    from tmp_up_from_temp_{self.table_name} d where lowrange = low_key_for_range and tablekey = table_key and effectivedate > effective_date
                    and b.app_date_end is null;
                    drop table if exists tmp_up_from_temp_{self.table_name};
                """
                
    def up_temp_from_dh(self)->str:
        """Query to update temporal table where efective date is lower than the old data and must have a final date, returns query as str."""
        return  f"""
                    update {self.schema}.{self.table_name} tem set app_date_end = sq.end_date - INTERVAL '1 DAY'
                    from (select a.low_key_for_range,a.table_key,a.effective_date,a.app_date_valid as "end_date"
                    from {self.main_schema}.{self.main_table}  a inner join 
                    (select lowrange,tablekey,min(exist_date) max_date from (select c.delete_indicator,c.low_key_for_range as "lowrange",c.table_key as "tablekey",
                    c.effective_date as "effectivedate", c.app_date_valid as "date_valid", b.effective_date as "exist_date"
                    from {self.main_schema}.{self.main_table}  b
                    left join {self.schema}.{self.table_name} c on b.low_key_for_range = c.low_key_for_range and b.table_key = c.table_key 
                    and b.effective_date > c.effective_date) sq group by lowrange,tablekey)b on  b.lowrange = a.low_key_for_range and b.tablekey = a.table_key and 
                    a.effective_date = max_date ) sq where sq.low_key_for_range = tem.low_key_for_range and sq.table_key = tem.table_key
                """
       

    def insert_into_dh(self,list_of_columns):
        """Query to insert new data into dh
        
        Args:
            list_of_columns (str): columns to beign inserted into table
        
        Returns:
            str: generated query
        
        """
        return f"""
                    insert into {self.main_schema}.{self.main_table}({list_of_columns}) select {list_of_columns} from
                    {self.schema}.{self.table_name} f inner join
                    (
                    select b.low_key_for_range as "lowrange",b.table_key as "tablekey",b.app_full_data as "compared_row"
                    from {self.main_schema}.{self.main_table} a
                    right join {self.schema}.{self.table_name} b on  b.low_key_for_range = a.low_key_for_range and b.table_key = a.table_key and a.app_full_data =b.app_full_data
                    where a.app_full_data is null
                    ) g on  g.compared_row = f.app_full_data;
                """
