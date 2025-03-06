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
        return f"""
                create temporary table {self.table_name}_subquery as (
                    select 
                        c.active_inactive_code,
                        c.low_range as lowrange,
                        c.gcms_product as gcmsproduct,
                        c.effective_timestamp as effectivetimestamp,
                        c.app_date_valid as date_valid
                    from {self.main_schema}.{self.main_table} b
                    inner join {self.schema}.{self.table_name} c on 
                    b.low_range = c.low_range and b.gcms_product = c.gcms_product 
                    and b.effective_timestamp < c.effective_timestamp 
                    and b.app_date_end is null
                );

                update {self.main_schema}.{self.main_table} b 
                    set app_date_end = (d.date_valid - INTERVAL '1 second')
                from 
                (
                    select * from {self.table_name}_subquery
                ) d 
                where lowrange = low_range 
                and gcmsproduct = gcms_product 
                and effectivetimestamp > effective_timestamp 
                and b.app_date_end is null;

                drop table if exists {self.table_name}_subquery;
                """
    def up_temp_from_dh(self)->str:
        """Query to update temporal table where efective date is lower than the old data and must have a final date, returns query as str."""
        return f"""
                update {self.schema}.{self.table_name} tem 
                    set app_date_end = (sq.end_date - INTERVAL '1 second')
                from 
                (
                select 
                    a.low_range,
                    a.gcms_product,
                    a.effective_timestamp,
                    a.app_date_valid as end_date
                from {self.main_schema}.{self.main_table} a inner join 
                    (select 
                        lowrange,
                        gcmsproduct,
                        min(exist_timestamp) max_timestamp 
                    from 
                        (select 
                            c.active_inactive_code,
                            c.low_range as lowrange,
                            c.gcms_product as gcmsproduct,
                            c.effective_timestamp as effectivetimestamp,
                            c.app_date_valid as "date_valid",
                            b.effective_timestamp as exist_timestamp
                        from {self.main_schema}.{self.main_table} b
                        left join {self.schema}.{self.table_name} c on
                        b.low_range = c.low_range and b.gcms_product = c.gcms_product 
                        and b.effective_timestamp > c.effective_timestamp) sq 
                        group by lowrange,gcmsproduct)b on  b.lowrange = a.low_range and b.gcmsproduct = a.gcms_product and 
                        a.effective_timestamp = max_timestamp ) sq 
                    where sq.low_range = tem.low_range 
                    and sq.gcms_product = tem.gcms_product
        """

    def insert_into_dh(self,list_of_columns):
        """Query to insert new data into dh
        
        Args:
            list_of_columns (str): columns to beign inserted into table
        
        Returns:
            str: generated query
        
        """
        return f"""
                insert into {self.main_schema}.{self.main_table}
                ({list_of_columns}) 
                select 
                    {list_of_columns} 
                from {self.schema}.{self.table_name} f inner join
                (
                    select 
                        b.low_range as lowrange,
                        b.gcms_product as gcmsproduct,
                        b.app_full_data as compared_row
                    from {self.main_schema}.{self.main_table} a
                right join {self.schema}.{self.table_name} b on  
                    b.low_range = a.low_range and b.gcms_product = a.gcms_product
                    and a.effective_timestamp = b.effective_timestamp
                where a.app_full_data is null
                ) g on g.compared_row = f.app_full_data;
        """