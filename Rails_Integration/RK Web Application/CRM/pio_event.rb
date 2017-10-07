###################################################################
#    Realtime storing of user/item event data in PIO eventserver  #
#            ########## Asynchronous ##############               #
#              Define enviroment variables in .envrc              #
###################################################################

require 'predictionio'

module Concerns::PioEvent
    extend ActiveSupport::Concern

#=== Start Eventserver
    client = PredictionIO::EventClient.new(ENV['PIO_ACCESS_KEY'])

#=== Import a User Record from app (with asynchronous/non-blocking request)

    # Call on GET X#activate_membership
    # POST /event.json
    def setUser
        client.aset_user(getUser)
    end
    
    # Call on GET X#cancel_membership
    # POST /event.json
    def unSetUser
        client.adelete_user(getUser)
    end
    

#=== Clean up

    # Select user from Users where ID = id
    def getUser
        Company.select(:user).find(params[:id])
    end

end

