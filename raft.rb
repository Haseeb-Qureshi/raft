require 'socket'
require 'json'
require 'set'
require 'thread'
require 'colorize'

STDOUT.sync = true
Thread.abort_on_exception = true

PORTS = [
  3001,
  3002,
  3003,
]

DEFAULT_INTERVAL = 1

LEADER = :leader
FOLLOWER = :follower
CANDIDATE = :candidate
VOTER = :voter

class Node
  attr_accessor :port_num, :state_data, :partitioned

  TRANSITIONS = {
    LEADER => {
      timeout: :review_last_heartbeat_round,
      acknowledgement: :log_heartbeat_reply,
      heartbeat: :respond_to_heartbeat,
      vote_request: :handle_vote_request,
    },
    FOLLOWER => {
      timeout: :launch_candidacy,
      heartbeat: :respond_to_heartbeat,
      vote_request: :handle_vote_request,
    },
    CANDIDATE => {
      timeout: :launch_candidacy,
      vote_request: :handle_vote_request,
      vote: :handle_vote,
      heartbeat: :respond_to_heartbeat,
    },
    VOTER => {
      timeout: :launch_candidacy,
      vote_request: :handle_vote_request,
      heartbeat: :respond_to_heartbeat,
    },
  }

  def initialize(port_num)
    @port_num = port_num
    @state_data = {
      leader: nil,
      round_num: 0,
      timeout: new_random_timeout,
      state: FOLLOWER,
    }
    @threads = []
    @queue = Queue.new
    @partitioned = false
  end

  def run
    t1 = Thread.new { listen }
    t2 = Thread.new { react }
    @threads = [t1, t2]
  end

  def join
    @threads.each(&:join)
  end

  def react
    loop do
      if rand < 1.fdiv(500) # once every 5 seconds
        @partitioned = !@partitioned
      end

      if Time.now.to_f > timeout
        handle_message!(Message.timeout_message(self))
      elsif message_queued?
        message = @queue.shift
        if message.expired?
          drop_message!(message)
        else
          handle_message!(message)
        end
      else
        sleep 0.01
      end
    end
  rescue => e
    global_puts [e.message, e.backtrace].join
  end

  def listen
    server = TCPServer.new(@port_num)
    loop do
      socket = server.accept
      raw_message = socket.gets
      raw_msg = JSON.parse(raw_message)
      msg = Message.new(
        raw_msg['transition'],
        raw_msg['round_num'],
        raw_msg['sender_port'],
        raw_msg['recipient_port']
      )

      if @partitioned
        drop_message!(msg, partitioned: true)
      else
        @queue << msg
      end
      socket.close
    end
  end

  private

  def message_queued?
    !@queue.empty?
  end

  def respond_to_heartbeat(msg)
    if msg.round_num >= state_data[:round_num]
      send_message(:acknowledgement, msg.sender_port)
      {
        leader: msg.sender_port,
        round_num: msg.round_num,
        timeout: new_random_timeout,
        state: FOLLOWER,
      }
    else
      state_data
    end
  end

  def log_heartbeat_reply(msg)
    {
      repliers: state_data[:repliers] + [msg.sender_port],
      round_num: msg.round_num,
      timeout: timeout,
      state: LEADER,
    }
  end

  def review_last_heartbeat_round(msg)
    ack_count = state_data[:repliers].size
    if !has_majority?(ack_count)
      launch_candidacy(msg)
    else
      send_heartbeats!
    end
  end

  def launch_candidacy(msg)
    (PORTS - [port_num]).each do |port|
      send_message(:vote_request, port, state_data[:round_num] + 1)
    end
    {
      voters: Set.new([port_num]),
      round_num: state_data[:round_num] + 1,
      timeout: new_random_timeout,
      state: CANDIDATE,
    }
  end

  def handle_vote_request(msg)
    if msg.round_num > @state_data[:round_num]
      send_message(:vote, msg.sender_port, msg.round_num)
      {
        round_num: msg.round_num,
        timeout: new_random_timeout,
        state: VOTER,
      }
    else
      state_data
    end
  end

  def handle_vote(msg)
    new_state = state_data.dup
    if msg.round_num != round_num
      drop_message!(msg)
    else
      new_state[:voters] += [msg.sender_port]
    end

    if has_majority?(new_state[:voters].count)
      send_heartbeats!
    else
      new_state
    end
  end

  def send_heartbeats!
    (PORTS - [port_num]).each do |port|
      send_message(:heartbeat, port)
    end
    {
      repliers: Set.new([port_num]),
      round_num: state_data[:round_num],
      timeout: new_random_timeout(LEADER),
      state: LEADER,
    }
  end

  def send_message(transition, recipient_port, round_num = state_data[:round_num])
    message = Message.new(transition, round_num, port_num, recipient_port)
    if @partitioned
      drop_message!(message, partitioned: true)
    else
      message.send!
    end
  end

  def handle_message!(message)
    transition = message.transition
    method_name = TRANSITIONS[@state_data[:state]][transition]
    debug = []
    debug << ["*******" * 5] * 2
    debug << Time.now.to_f.to_s.yellow
    debug << "Message received by #{port_num}: #{message.to_json}"
    debug << "Current state: #{@state_data[:state]}"
    debug << transition.to_s.blue
    debug << @state_data.to_s
    if TRANSITIONS[@state_data[:state]].key?(transition)
      @state_data = send(method_name, message)
      debug << @state_data.to_s
    else
      drop_message!(message)
    end
    debug << ["*******" * 5] * 2
    debug << "\n"
    global_puts debug.flatten.join("\n")
  end

  def drop_message!(message, partitioned: false)
    output = []
    output << "Dropping message!!! #{message.to_json}".red
    output << "(I've been partitioned...)" if partitioned
    global_puts output.join("\n")
  end

  def has_majority?(count)
    count >= (PORTS.count / 2) + 1
  end

  def new_random_timeout(role = nil)
    multiplier = role == LEADER ? 1 : 2

    Time.now.to_f + DEFAULT_INTERVAL * (1 + rand) * multiplier
  end

  def timeout
    @state_data[:timeout]
  end

  def round_num
    @state_data[:round_num]
  end
end

class Message
  attr_accessor :transition, :round_num, :sender_port, :recipient_port, :timestamp

  def initialize(transition, round_num, sender_port, recipient_port)
    @transition = transition.to_sym
    @round_num = round_num
    @sender_port = sender_port
    @recipient_port = recipient_port
    @timestamp = Time.now.to_f
  end

  def self.timeout_message(node)
    self.new(:timeout, node.state_data[:round_num], node.port_num, node.port_num)
  end

  def send!
    Thread.new do
      socket = TCPSocket.new('localhost', recipient_port)
      socket.puts(self.to_json)
      socket.close
    end
  end

  def expired?
    Time.now.to_f - timestamp > 1.0
  end

  def to_json
    {
      transition: transition,
      round_num: round_num,
      sender_port: sender_port,
      recipient_port: recipient_port,
      timestamp: timestamp,
    }.to_json
  end
end

DEBUG_QUEUE = Queue.new

def global_puts(msg)
  DEBUG_QUEUE.push(msg)
  DEBUG_QUEUE.push(global_state)
end

def global_state
  NODES.map do |node|
    s = "Node #{node.port_num}: "\
    "#{node.state_data[:state]}, "\
    "round: #{node.send(:round_num)}".green
    if node.partitioned
      s << "---PARTITIONED---".red
    end
    s
  end
end

NODES = PORTS.map { |port| Node.new(port) }
NODES.each(&:run)
loop do
  puts DEBUG_QUEUE.shift
end
