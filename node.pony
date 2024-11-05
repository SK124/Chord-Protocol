use "collections"
use "random"

//Performs the actual search, send_request behaviour searches for the key in the ring. 
actor Node
    let _env: Env
    let id: U64
    var predecessor_id: U64
    var successor_id: U64
    let main: Main 
    var finger_table: Map[U64, Node] val 
    var finger_keys : Array[U64] val
    
    new create(id': U64, main': Main, env: Env) =>
        _env = env
        main = main'
        id = id'
        predecessor_id = 0
        successor_id = 0
        finger_table = Map[U64, Node]
        finger_keys = Array[U64]
        
    be set_successor(succ: U64) =>
        successor_id = succ
    
    be set_predecessor(pred: U64) =>
        predecessor_id = pred
    
    fun get_id() =>
        id

    be set_finger_table(finger_table': Map[U64, Node tag] val, finger_keys': Array[U64] val) =>
        finger_table = finger_table'
        finger_keys = finger_keys'
        // I set the successor key using the finger table
        try 
            this.set_successor(finger_keys(0)?)
        end

    be send_request(key_id: U64, hops: U64) => 
        // If my predecessor is wrapped around and key is less than current node
        if (predecessor_id > id) and (key_id <= id) then
            main.notify_hops(hops)
            return
        end

        // If the key_id belongs to the current node, update
        if (predecessor_id < key_id) and (key_id <= id) then
            main.notify_hops(hops)
        
        // Else if it belongs to the current and neighbor
        else if (id < key_id) and (key_id <= successor_id) then
            main.notify_hops(hops + 1)
        
        else
            try
                // If the value is larger than the largest successor, delegate the work to that node
                var largest_predecessor_id: U64 = finger_keys(finger_keys.size() - 1)?
                var largest_predecessor: Node = finger_table(largest_predecessor_id)?
            
                // Find the largest predecessor whose id is lesser than key_id,
                // delegate the work to the predecessor node
                for i in Range(1, finger_table.size()) do
                    let curr_node_id = finger_keys(i - 1)?
                    let next_node_id = finger_keys(i)?

                    // In case of wrap-around, compare if the key falls between the left and right
                    // If it does, give the key to right, else keep iterating
                    if curr_node_id > next_node_id then 
                        if (key_id < next_node_id) or (key_id >= curr_node_id) then
                            largest_predecessor = finger_table(curr_node_id)?
                            break
                        end
                    end

                    if (curr_node_id <= key_id) and (key_id < next_node_id) then
                        largest_predecessor = finger_table(curr_node_id)?
                        break
                    end
                end
                largest_predecessor.send_request(key_id, 1 + hops)
            else 
                _env.out.print("Error in sending request")
            end
        end
    end

