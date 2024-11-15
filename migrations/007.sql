CREATE TABLE IF NOT EXISTS ner_unstructured_data
(
    id UUID DEFAULT generateUUIDv4(),          
    source_bucket String,                      
    file_name String,                         
    json String,                               
    file_size UInt64,                          
    file_type String,  
    source String,
    region String,                        
    created_at DateTime DEFAULT now(),         
    updated_at DateTime DEFAULT now()          
) 
ENGINE = MergeTree()
PARTITION BY source_bucket                     
ORDER BY id;                                  