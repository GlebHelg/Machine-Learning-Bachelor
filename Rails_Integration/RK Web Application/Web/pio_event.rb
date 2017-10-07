###################################################################
#         Realtime storing of item event data in PIO eventserver  #
#            ########## Asynchronous ##############               #
#              Define environment variables in .envrc              #
###################################################################
 
require 'predictionio'

module Concerns::PioPushEvent
    extend ActiveSupport::Concern

#=== Start Eventserver
    client = PredictionIO::EventClient.new(ENV['PIO_ACCESS_KEY'])

#=== Import a User Record from app (with asynchronous/non-blocking request)


    # Call on GET admin/products#activate  action
    # set_item(iid, properties = {}, optional = {})
    # POST /event.json 
    def setItem
        client.aset_item(getItem, getProperties)
    end
    
    # Call on GET admin/products#deactivate  action
    # aunset_item(iid, properties, optional = {})
    # POST /event.json
    def unSetItem
        client.aunset_item(getItem, getProperties)
    end
    


#=== Events 
    
    # Record user event actions
    # arecord_user_action_on_item(action, uid, iid, optional = {})
    #
    # Call on GET products#show action
    # POST /event.json
    def viewEvent
        client.arecord_user_action_on_item('view', getUser, getItem)       
    end
    
    # GET 	/K/O/:order_id(.:format)        member/checkouts#confirmation  
    # POST /event.json
    def buyEvent
        client.arecord_user_action_on_item('buy', getUser, getCart) 
    end
    

#=== Clean up

    # Select Users.user from Users CRM
    def getUser
        current_company.user
    end
    
    # Select Products.product_id from Products where id='x'
    def getItem
        Product.select(:product).find(params[:id])
    end

    # Select Category.id from Category where id='x'
    def getProperties
        Category.select(:id)
    end
    
    # Select Product.product_id  FROM cart_items JOIN Product WHERE shopping_cart.item_id = Product.product_id AND shopping_cart.id='x'
    def getCart
        params.fetch(:customer_order, {}).permit()
    end
end

