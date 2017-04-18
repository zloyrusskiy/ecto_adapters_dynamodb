defmodule Ecto.Adapters.DynamoDB do
#defmodule DynamoDB.Ecto do
  @moduledoc """
  Ecto adapter for Amazon DynamoDB

  Currently fairly limited subset of Ecto, enough for basic operations.

  NOTE: in ecto, Repo.get[!] ends up calling: 
    -> querable.get
    -> queryable.one
    -> queryable.all
    -> queryable.execute
    -> adapter.execute (possibly prepare somewhere in their too? trace.)


  """



  @behaviour Ecto.Adapter
  #@behaviour Ecto.Adapter.Storage
  #@behaviour Ecto.Adapter.Migration

  defmacro __before_compile__(_env) do
    # Nothing to see here, yet...

  end


  # I don't think this is necessary: Probably under child_spec and ensure_all_started
  def start_link(repo, opts) do
    IO.puts("start_link repo: #{inspect repo} opts: #{inspect opts}")
    Agent.start_link fn -> [] end
  end


  ## Adapter behaviour - defined in lib/ecto/adapter.ex (in the ecto github repository)

  @doc """
  Returns the childspec that starts the adapter process.
  """
  def child_spec(repo, opts) do
    # TODO: need something here...
    # * Pull dynamo db connection options from config
    # * Start dynamo connector/aws libraries
    # we'll return our own start_link for now, but I don't think we actually need
    # an app here, we only need to ensure that our dependencies such as aws libs are started.
    # 
    import Supervisor.Spec
    child_spec = worker(__MODULE__, [repo, opts])
    IO.puts("child spec3. REPO: #{inspect repo}\n CHILD_SPEC: #{inspect child_spec}\nOPTS: #{inspect opts}")
    child_spec
  end


  @doc """
  Ensure all applications necessary to run the adapter are started.
  """
  def ensure_all_started(repo, type) do
    IO.puts("ensure all started: type: #{inspect type} #{inspect repo}")
    {:ok, [repo]}
  end


# moved to transaction.ex in ecto 2.1.4
#  def in_transaction?(_repo), do: false
#
#  def rollback(_repo, _value), do:
#    raise BadFunctionError, message: "#{inspect __MODULE__} does not support transactions."


  @doc """
  Called to autogenerate a value for id/embed_id/binary_id.

  Returns the autogenerated value, or nil if it must be
  autogenerated inside the storage or raise if not supported.
  """

  def autogenerate(:id), do: Ecto.UUID.bingenerate()
  def autogenerate(:embed_id), do: Ecto.UUID.generate()
  def autogenerate(:binary_id), do: Ecto.UUID.bingenerate()

  @doc """
  Returns the loaders for a given type.

  It receives the primitive type and the Ecto type (which may be
  primitive as well). It returns a list of loaders with the given
  type usually at the end.

  This allows developers to properly translate values coming from
  the adapters into Ecto ones. For example, if the database does not
  support booleans but instead returns 0 and 1 for them, you could
  add:

    def loaders(:boolean, type), do: [&bool_decode/1, type]
    def loaders(_primitive, type), do: [type]

    defp bool_decode(0), do: {:ok, false}
    defp bool_decode(1), do: {:ok, true}

  All adapters are required to implement a clause for `:binary_id` types,
  since they are adapter specific. If your adapter does not provide binary
  ids, you may simply use Ecto.UUID:

    def loaders(:binary_id, type), do: [Ecto.UUID, type]
    def loaders(_primitive, type), do: [type]

  """
  def loaders(_primative, type), do: [type]



  @doc """
  Returns the dumpers for a given type.

  It receives the primitive type and the Ecto type (which may be
  primitive as well). It returns a list of dumpers with the given
  type usually at the beginning.

  This allows developers to properly translate values coming from
  the Ecto into adapter ones. For example, if the database does not
  support booleans but instead returns 0 and 1 for them, you could
  add:

    def dumpers(:boolean, type), do: [type, &bool_encode/1]
    def dumpers(_primitive, type), do: [type]

    defp bool_encode(false), do: {:ok, 0}
    defp bool_encode(true), do: {:ok, 1}

  All adapters are required to implement a clause or :binary_id types,
  since they are adapter specific. If your adapter does not provide
  binary ids, you may simply use Ecto.UUID:

    def dumpers(:binary_id, type), do: [type, Ecto.UUID]
    def dumpers(_primitive, type), do: [type]

  """
  def dumpers(_primative, type), do: [type]



  @doc """
  Commands invoked to prepare a query for `all`, `update_all` and `delete_all`.

  The returned result is given to `execute/6`.
  """
  #@callback prepare(atom :: :all | :update_all | :delete_all, query :: Ecto.Query.t) ::
  #          {:cache, prepared} | {:nocache, prepared}
  def prepare(:all, query) do
    # 'preparing' is more a SQL concept - Do we really need to do anything here or just pass the params through?
    IO.puts("PREPARE:::")
    IO.inspect(query)
    {:nocache, query}
  end
  #def prepare(:update_all, query),
  # do: {:cache, {System.unique_integer([:positive]), @conn.update_all(query)}}
  #def prepare(:delete_all, query),
  # do: {:cache, {System.unique_integer([:positive]), @conn.delete_all(query)}}



  @doc """
  Executes a previously prepared query.

  It must return a tuple containing the number of entries and
  the result set as a list of lists. The result set may also be
  `nil` if a particular operation does not support them.

  The `meta` field is a map containing some of the fields found
  in the `Ecto.Query` struct.

  It receives a process function that should be invoked for each
  selected field in the query result in order to convert them to the
  expected Ecto type. The `process` function will be nil if no
  result set is expected from the query.
  """
  #@callback execute(repo, query_meta, query, params :: list(), process | nil, options) :: result when
  #          result: {integer, [[term]] | nil} | no_return,
  #          query: {:nocache, prepared} |
  #                 {:cached, (prepared -> :ok), cached} |
  #                 {:cache, (cached -> :ok), prepared}

  def execute(_repo, _meta, {:nocache, _prepared}, _params, _process = nil, _opts) do
    #Logger.error "EXECUTE... EXECUTING!"
    IO.puts "EXECUTE... EXECUTING1!"
    IO.puts("execute: \nprepared:#{inspect _prepared}\nparams: #{inspect _params}\nopts: #{inspect _opts}")
    num = 0
    rows = []
    {num, rows}
  end

  def execute(repo, meta, {:nocache, prepared}, params, process, opts) do
    IO.puts "EXECUTE... EXECUTING!============================="
    IO.puts "REPO::: #{inspect repo}"
    IO.puts "META::: #{inspect meta}"
    IO.puts "PREPARED::: #{inspect prepared}"
    IO.puts "PARAMS::: #{inspect params}"
    IO.puts "PROCESS::: #{inspect process}"
    IO.puts "OPTS::: #{inspect opts}"

    dump_query_struct(prepared)

    raise BadFunctionError, message: "#{inspect __MODULE__}.execute is not implemented."


    num = 0
    rows = []
    {num, rows}
  end


  @doc """
  Inserts a single new struct in the data store.

  ## Autogenerate

  The primary key will be automatically included in `returning` if the
  field has type `:id` or `:binary_id` and no value was set by the
  developer or none was autogenerated by the adapter.
  """
  #@callback insert(repo, schema_meta, fields, on_conflict, returning, options) ::
  #                  {:ok, fields} | {:invalid, constraints} | no_return
  #  def insert(_,_,_,_,_) do
  def insert(repo, schema_meta, fields, on_conflict, returning, options) do
    IO.puts("INSERT::\n\trepo: #{inspect repo}")
    IO.puts("\tschema_meta: #{inspect schema_meta}")
    IO.puts("\tfields: #{inspect fields}")
    IO.puts("\ton_conflict: #{inspect on_conflict}")
    IO.puts("\treturning: #{inspect returning}")
    IO.puts("\toptions: #{inspect options}")
    raise BadFunctionError, message: "#{inspect __MODULE__}.insert is not implemented."
  end

  def delete(_,_,_,_), do: raise BadFunctionError, message: "#{inspect __MODULE__}.delete is not implemented."
  def insert_all(_,_,_,_,_,_,_), do: raise BadFunctionError, message: "#{inspect __MODULE__}.insert_all is not implemented."
  def update(_,_,_,_,_,_), do: raise BadFunctionError, message: "#{inspect __MODULE__}.update is not implemented."



defp dump_query_struct(struct) do
  IO.puts("DUMPING QUERY STRUCT - ")
IO.puts("   struct.prefix: #{inspect  struct.prefix}")
IO.puts("   struct.sources: #{inspect  struct.sources}")
IO.puts("   struct.from: #{inspect  struct.from}")
IO.puts("   struct.joins: #{inspect  struct.joins}")
IO.puts("   struct.wheres: #{inspect  struct.wheres}")
IO.puts("   struct.select: #{inspect  struct.select}")
IO.puts("   struct.order_bys: #{inspect  struct.order_bys}")
IO.puts("   struct.limit: #{inspect  struct.limit}")
IO.puts("   struct.offset: #{inspect  struct.offset}")
IO.puts("   struct.group_bys: #{inspect  struct.group_bys}")
IO.puts("   struct.updates: #{inspect  struct.updates}")
IO.puts("   struct.havings: #{inspect  struct.havings}")
IO.puts("   struct.preloads: #{inspect  struct.preloads}")
IO.puts("   struct.assocs: #{inspect  struct.assocs}")
IO.puts("   struct.distinct: #{inspect  struct.distinct}")
IO.puts("    struct.lock: #{inspect   struct.lock}")
end

end



