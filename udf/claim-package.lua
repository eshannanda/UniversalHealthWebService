local function map_profile(record)
  -- Add user and password to returned map.
  -- Could add other record bins here as well.
  return map {Hospital=record.Hospital, Claim_DATE=record.Claim_DATE}
end

function check_claim_count(stream,hId)
  local function filter_Hospital(record)
    return record.Hospital == hId
  end
  return stream : filter(filter_Hospital) : map(map_profile)
end