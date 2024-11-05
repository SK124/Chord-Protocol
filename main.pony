use "collections"
use "time"
use "random"

// Main Actor 
// Performs the following:
// 1. Builds Network, dynamically creates finger table for every neighbour
// 2. Sends requests at the rate of one request per second to all nodes
// 3. Node's update the hops to the main actor 

actor Main
    let _env: Env
    var messages: Array[U64] = []
    var network_size: U64 = 0
    var finger_table_size: U64 = 0
    var num_nodes: U64 = 0
    var num_requests: U64 = 0
    let nodes: Map[U64, Node] = Map[U64, Node]
    let picked: Set[USize] = Set[USize]
    var completed_requests: U64 = 0
    var total_hops: U64 = 0
    
    new create(env: Env) =>
        _env = env
        try
            if env.args.size() != 3 then
                error    
            end
            num_nodes = env.args(1)?.u64()?
            num_requests = env.args(2)?.u64()?
            setup_chord_network()
        else
            _env.out.print("Error in parsing given command line args")
        end

    fun ref create_network() =>
        let interval: F64 = network_size.f64() / num_nodes.f64()
        
        var i: U64 = 0
        while i < num_nodes do
            let position = (i.f64() * interval).round().u64()
            let new_node = Node(position, this, _env)

            nodes(position) = new_node

            if i != 0 then 
                var pre: U64 = ((i.f64() - 1) * interval).round().u64()
                try nodes(position)?.set_predecessor(pre) end
            end


            i = i + 1
        end
        var pred_of_first = ((i.f64() - 1) * interval).round().u64()

        try nodes(0)?.set_predecessor(pred_of_first) end


        _env.out.print("Finger table size: " + finger_table_size.string())

        for node_id in nodes.keys() do
            let finger_table: Map[U64, Node] iso = Map[U64, Node]
            let finger_table_keys: Array[U64] iso = Array[U64]
            

            var j: U64 = 0
            while j < finger_table_size do
                let jump = U64(1) << j  // 2^j
                let target_id = (node_id + jump) % network_size
                
                let successor_id = find_successor(target_id)

                try
                    if not finger_table.contains(successor_id) then 
                        finger_table_keys.push(successor_id)
                    end

                    finger_table(successor_id) = nodes(successor_id)?
                end
                
                j = j + 1
            end

            try
                nodes(node_id)?.set_finger_table(consume finger_table, consume finger_table_keys)
            end
        end


    fun get_max(a: U64, b: U64): U64 =>
        if a > b 
            then a else b 
        end

    fun ref get_network_size(): U64 =>
        var size: U64 = 1
        var count: U64 = 0
        let maxlimit: U64 = get_max(num_nodes, num_requests)
        while size <= maxlimit do
            size = size * 2
            count = count + 1
        end
        finger_table_size = count
        size


    fun ref find_successor(id: U64): U64 =>
        var successor_id = id
        
        while not nodes.contains(successor_id) do
            successor_id = (successor_id + 1) % network_size
        end
        
        successor_id

    
    fun ref generate(count: USize, min: U64, max: U64): Array[U64] =>
        
        let range = max - (min + 1)
        let rand = Rand(Time.nanos())

        if count > range.usize() then
            return Array[U64]
        end

        let numbers = Array[U64](range.usize())
        var i: U64 = min
        while i <= max do
            numbers.push(i)
            i = i + 1
        end

        let result = Array[U64](count)
        var remaining = range.usize()
        try
            while result.size() < count do
                let index = rand.int(remaining.u64()).usize()
                result.push(numbers(index)?) 

                numbers(index)? = numbers(remaining - 1)?
                remaining = remaining - 1
            end
        end
        
        result


    be notify_hops(hops': U64) =>
        total_hops = total_hops + hops'
        completed_requests = completed_requests + 1
        _env.out.print("Current Hops: " + completed_requests.string()+ " Expected Hops: " + (num_requests*num_nodes).string())
        if completed_requests >= (num_requests*num_nodes) then
            // Calculating Average
            let average: F64 = total_hops.f64() / completed_requests.f64()
            _env.out.print("Average Hops: " + average.string())
        end
        

    fun ref setup_chord_network() =>
        network_size = get_network_size()
        _env.out.print("Creating a Network Of Size:  " + network_size.string())

        create_network()

        messages = generate(num_requests.usize(), 0, network_size)

        let timers = Timers
        let timer = Timer(GenerateNumber(_env, messages.size().u64(), this), 0, 1_000_000_000)
        timers(consume timer)

    be send_message(index: U64) => 

        try 
            for node_id in nodes.keys() do
                nodes(node_id)?.send_request(messages((index-1).usize())?.u64(), 0)
            end
        end


// Timer class to send requests 
class GenerateNumber is TimerNotify
  let _env: Env
  var counter: U64
  var threshold: U64 = 4
  var main: Main

  new iso create(env': Env, threshold': U64, main': Main) =>
    counter = 0
    _env = env'
    threshold = threshold'
    main = main'

  fun ref next(): String =>
    counter = counter + 1
    main.send_message(counter)
    counter.string()

  fun ref apply(timer: Timer, count: U64): Bool =>
    if counter >= threshold then 
      return false  
    end

    _env.out.print(next())
    true