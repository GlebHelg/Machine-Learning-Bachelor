# Query module
# API: https://github.com/apache/incubator-predictionio-sdk-ruby/blob/develop/lib/predictionio/engine_client.rb


require 'predictionio'

module Conserns::PioGetQuery
    extend ActiveSupport::Consern

#=== Instantiate EngineClient
    client = PredictionIO::EngineClient.new


# Send Query to retrieve Predicted items for product recommendation table
# 
# query('user.id' => 'amount of predicted items return')
# Occurance: Every instance of M/C#show action

    def pioQuery
        client.query(current_company.user => 4)
    end
    
end
