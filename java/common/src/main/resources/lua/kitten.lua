
-- Returns a function that will concatenate the
-- key-value pairs from the given table with the
-- any table that is given as an input to the returned function.
function cat(t)
  return function(table)
    for k, v in pairs(t) do
      table[k] = table[k] or v
    end
    return table
  end
end

-- Verifications that ensure that a table has all of
-- the fields required for creating a YARN application. 
function yarn(t)
  -- Start with basic config checks and default settings
  if not t.name then
    error("Every yarn configuration must have an app name")
  end
  if not t.master then
    error("Every yarn configuration must specify a master")
  end

  local t_check = function(v, field, expected_type)
    if "function" == type(v) then
      v = v()
    end
    if expected_type ~= type(v) then
      error("Field " .. field .. " not of type " .. expected_type .. ": " .. type(v))
    end
    return v
  end

  t.name = t_check(t.name, "name", "string")
  t.queue = t_check(t.queue or "default", "queue", "string")
  t.user = t_check(t.user or "", "user", "string")
  t.timeout = t_check(t.timeout or -1, "timeout", "number")

  local f_check = function(f, field)
    if "function" == type(f) then
      f = f()
    end
    if "table" ~= type(f) then
      error("The " .. field .. " of a yarn instance must return a table")
    end
    setmetatable(f, { __index = t })
    return f
  end

  local s_check = function(v, name)
    if "function" == type(v) then
      v = v()
    end
    return tostring(v)
  end

  -- Checks and defaults for a ContainerLaunchParameters.
  local clp_check = function(clp, name)
    clp = f_check(clp, name)
    clp.user = t_check(clp.user, name .. ".user", "string")
    clp.memory = t_check(clp.memory or 512, name .. ".memory", "number")
    clp.cores = t_check(clp.cores or 1, name .. ".cores", "number")
    clp.priority = t_check(clp.priority or 0, name .. ".priority", "number")
    clp.instances = t_check(clp.instances or 1, name .. ".instances", "number")

    clp.env = t_check(clp.env or {}, name .. ".env", "table")
    for k, v in pairs(clp.env) do
      clp.env[k] = s_check(v, name .. ".env[" .. k .. "]")
    end

    -- What to do for command and for resources?
    return clp
  end

  -- Verify that the master is configured correctly.
  t.master = clp_check(t.master, "master")

  -- Verify the container(s) configuration.
  if t.container then
    t.container = clp_check(t.container, "node")
  elseif t.containers then
    -- Need to add checks here to ensure these exist.
    for i, container in ipairs(t.containers) do
      local container_name = "container(" .. i .. ")"
      t.containers[i] = clp_check(t.containers[i], container_name)
    end
  end

  return t
end

-- Constructs a java commandline argument from the fields
-- in the given table.
--
-- Supported table fields:
-- java: The path to the java executable. ${JAVA_HOME}/bin/java by default.
-- jvm_args: A table of optional JVM arguments, minus the '-' prefix. Values
-- that take arguments should be key-value pairs.
-- main: The name of the class containing the main method to run the package.
-- args: A table of arguments to the main(String[] args) value. Commandline flags
-- should be represented as key-value pairs.
-- prefix: The commandline flag prefix string, '-' by default.
-- disable_logging: Turns off logging of stdout and stderr to the YARN logs
-- directory for the container.
function java_cmd(t)
  if t.java then
    base = t.java
  else
    base = "${JAVA_HOME}/bin/java"
  end
  if t.jvm_args then
    for k, v in pairs(t.jvm_args) do
      if type(k) == "string" then
        base = base .. " -" .. k .. "=" .. v
      end
    end
    for i, v in ipairs(t.jvm_args) do
      base = base .. " -" .. v
    end
  end
  if t.main then
    base = base .. " " .. t.main
  end
  if t.args then
    if t.prefix then
      prefix = t.prefix
    else
      prefix = "-"
    end
    for k, v in pairs(t.args) do
      if type(k) == "string" and v ~= nil then
        base = base .. " " .. prefix .. k .. "=" .. v
      end
    end
    for i, v in ipairs(t.args) do
      if v ~= nil then
        base = base .. " " .. v
      end
    end
  end
  if t.disable_logging then
    base = base .. " 1> <LOG_DIR>/stdout 2> <LOG_DIR>/stderr"
  end
  return base
end
