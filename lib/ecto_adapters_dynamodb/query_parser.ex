defmodule Ecto.Adapters.DynamoDB.QueryParser do
  alias Ecto.Query.BooleanExpr

  @type operator :: :== | :< | :> | :<= | :>=
  @type field_name :: String.t()
  @type field_value :: term()
  @type lookup_fields :: list({field_name, {field_value, operator}})
  @type query_expression_list :: list(term())
  @type query_parameters :: list(field_value)

  # We are parsing a nested, recursive structure of the general type:
  # %{:logical_op, list_of_clauses} | %{:conditional_op, field_and_value}
  @spec extract_lookup_fields(query_expression_list(), query_parameters()) :: lookup_fields
  def extract_lookup_fields(queries, params), do: extract_lookup_fields(queries, params, [])
  def extract_lookup_fields([], _params, lookup_fields), do: lookup_fields
  def extract_lookup_fields([query | queries], params, lookup_fields) do
    # A logical operator tuple does not always have a parent 'expr' key.
    maybe_extract_from_expr = case query do
      %BooleanExpr{expr: expr} -> expr
      # TODO: could there be other cases?
      _                        -> query
    end

    case maybe_extract_from_expr do
      # A logical operator points to a list of conditionals
      {op, _, [left, right]} when op in [:==, :<, :>, :<=, :>=, :in] ->
        {field, value} = get_op_clause(left, right, params)

        updated_lookup_fields =
          case List.keyfind(lookup_fields, field, 0) do
            # we assume the most ops we can apply to one field is two, otherwise this might throw an error
            {field, {old_val, old_op}} ->
              List.keyreplace(lookup_fields, field, 0, {field, {[value, old_val], [op, old_op]}})

            _ -> [{field, {value, op}} | lookup_fields]
          end
        extract_lookup_fields(queries, params, updated_lookup_fields)

      # Logical operator expressions have more than one op clause
      # We are matching queries of the type: 'from(p in Person, where: p.email == "g@email.com" and p.first_name == "George")'
      # But not of the type: 'from(p in Person, where: [email: "g@email.com", first_name: "George"])'
      #
      # A logical operator is a member of a list
      {logical_op, _, clauses} when logical_op in [:and, :or] ->
        extract_lookup_fields(clauses, params, lookup_fields)

      {:fragment, _, raw_expr_mixed_list} ->
        parsed_fragment = parse_raw_expr_mixed_list(raw_expr_mixed_list, params)
        extract_lookup_fields(queries, params, [parsed_fragment | lookup_fields])

      # We perform a post-query is_nil filter on indexed fields and have DynamoDB filter
      # for nil non-indexed fields (although post-query nil-filters on (missing) indexed
      # attributes could only find matches when the attributes are not the range part of
      # a queried partition key (hash part) since those would not return the sought records).
      {:is_nil, _, [arg]} ->
        {{:., _, [_, field_name]}, _, _} = arg

        # We give the nil value a string, "null", since it will be mapped as a DynamoDB attribute_expression_value
        extract_lookup_fields(queries, params, [{to_string(field_name), {"null", :is_nil}} | lookup_fields])

      _ -> extract_lookup_fields(queries, params, lookup_fields)
    end
  end

  def extract_select_fields(%Ecto.Query.SelectExpr{fields: fields}) do
    fields
    |> Enum.map(fn select_field ->
      case select_field do
        {{:., _, [{_, _, _}, field]}, _, _} ->
          field

        %Ecto.Query.Tagged{value: {{_, _, [_, field]}, _, _}} -> field
      end
    end)
  end

  defp get_op_clause(left, right, params) do
    field = left |> get_field |> Atom.to_string
    value = get_value(right, params)

    {field, value}
  end

  defp get_field({{:., _, [{:&, _, [0]}, field]}, _, []}), do: field
  defp get_field(other_clause) do
    error "Unsupported where clause, left hand side: #{other_clause}"
  end

  defp get_value(%Ecto.Query.Tagged{value: {:^, _, [idx]}}, params), do: Enum.at(params, idx)
  defp get_value(%Ecto.Query.Tagged{value: value}, _params), do: value
  defp get_value({:type, _, [{:^, _, [idx]}, _]}, params), do: Enum.at(params, idx)
  defp get_value({:^, _, [idx]}, params), do: Enum.at(params, idx)
  # Handle queries with variable values, ex. Repo.all from i in Item, where: i.id in ^item_ids
  # The last element of the tuple (first arg) will be a list with two numbers;
  # the first number will be the number of attributes to be updated (in the event of an update_all query with a variable list)
  # and the second will be a count of the number of elements in the variable list being queried. For example:
  #
  # query = from p in Person, where: p.id in ^ids
  # TestRepo.update_all(query, set: [password: "cheese", last_name: "Smith"])
  #
  # assuming that ids contains 4 values, the last element would be [2, 4].
  # Use this data to modify the params, which would otherwise include the values to be updated as well, which we don't want to query on.
  defp get_value({:^, _, [num_update_terms, _num_query_terms]}, params), do: Enum.drop(params, num_update_terms)
  # Handle .all(query) queries
  defp get_value(other_clause, _params), do: other_clause

    # Specific (as opposed to generalized) parsing for Ecto :fragment - the only use for it
  # so far is 'between' which is the only way to query 'between' on an indexed field since
  # those accept only single conditions.
  #
  # Example with values as strings: [raw: "", expr: {{:., [], [{:&, [], [0]}, :person_id]}, [], []}, raw: " between ", expr: "person:a", raw: " and ", expr: "person:f", raw: ""]
  #
  # Example with values as part of the string itself: [raw: "", expr: {{:., [], [{:&, [], [0]}, :person_id]}, [], []}, raw: " between person:a and person:f"]
  #
  # Example with values in params: [raw: "", expr: {{:., [], [{:&, [], [0]}, :person_id]}, [], []}, raw: " between ", expr: {:^, [], [0]}, raw: " and ", expr: {:^, [], [1]}, raw: ""]
  #
  defp parse_raw_expr_mixed_list(raw_expr_mixed_list, params) do
    # group the expression into fields, values, and operators,
    # only supporting the example with values in params
    case raw_expr_mixed_list do
      # between
      [raw: _, expr: {{:., [], [{:&, [], [0]}, field_atom]}, [], []}, raw: between_str, expr: {:^, [], [idx1]}, raw: and_str, expr: {:^, [], [idx2]}, raw: _] ->
        if not (Regex.match?(~r/^\s*between\s*and\s*$/i, between_str <> and_str)), do:
          parse_raw_expr_mixed_list_error(raw_expr_mixed_list)
        {to_string(field_atom), {[Enum.at(params, idx1), Enum.at(params, idx2)], :between}}

      # begins_with
      [raw: begins_with_str, expr: {{:., [], [{:&, [], [0]}, field_atom]}, [], []}, raw: comma_str, expr: {:^, [], [idx]}, raw: closing_parenthesis_str] ->
        if not (Regex.match?(~r/^\s*begins_with\(\s*,\s*\)\s*$/i, begins_with_str <> comma_str <> closing_parenthesis_str)), do:
          parse_raw_expr_mixed_list_error(raw_expr_mixed_list)
        {to_string(field_atom), {Enum.at(params, idx), :begins_with}}

      _ -> parse_raw_expr_mixed_list_error(raw_expr_mixed_list)
    end
  end

  defp parse_raw_expr_mixed_list_error(raw_expr_mixed_list), do:
    raise "#{inspect __MODULE__}.parse_raw_expr_mixed_list parse error. We currently only support the Ecto fragments of the form, 'where: fragment(\"? between ? and ?\", FIELD_AS_VARIABLE, VALUE_AS_VARIABLE, VALUE_AS_VARIABLE)'; and 'where: fragment(\"begins_with(?, ?)\", FIELD_AS_VARIABLE, VALUE_AS_VARIABLE)'. Received: #{inspect raw_expr_mixed_list}"

  defp error(msg) do
    raise ArgumentError, message: msg
  end
end