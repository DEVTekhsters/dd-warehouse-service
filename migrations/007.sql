CREATE TABLE IF NOT EXISTS ner_unstructured_data
(
    id UUID DEFAULT generateUUIDv4(),          
    source_bucket String,                      
    file_name String,   
    column_name String,                      
    json String,  
    detected_entity String,
    data_element String, 
    data_sensitivity String,                            
    file_size UInt64,                          
    file_type String,  
    source String,
    sub_service String,
    region String,                        
    created_at DateTime DEFAULT now(),         
    updated_at DateTime DEFAULT now()          
) 
ENGINE = MergeTree()
PARTITION BY source_bucket                     
ORDER BY id;                                  