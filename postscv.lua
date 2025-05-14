local urlparse = require("socket.url")
local http = require("socket.http")
local https = require("ssl.https")
local cjson = require("cjson")
local utf8 = require("utf8")
local html_entities = require("htmlEntities")

local item_dir = os.getenv("item_dir")
local warc_file_base = os.getenv("warc_file_base")
local concurrency = tonumber(os.getenv("concurrency"))
local item_type = nil
local item_name = nil
local item_value = nil
local item_user = nil

local url_count = 0
local tries = 0
local downloaded = {}
local seen_200 = {}
local addedtolist = {}
local abortgrab = false
local killgrab = false
local logged_response = false

local discovered_outlinks = {}
local discovered_items = {}
local bad_items = {}
local ids = {}

local retry_url = false
local is_initial_url = true

abort_item = function(item)
  abortgrab = true
  --killgrab = true
  if not item then
    item = item_name
  end
  if not bad_items[item] then
    io.stdout:write("Aborting item " .. item .. ".\n")
    io.stdout:flush()
    bad_items[item] = true
  end
end

kill_grab = function(item)
  io.stdout:write("Aborting crawling.\n")
  killgrab = true
end

read_file = function(file)
  if file then
    local f = assert(io.open(file))
    local data = f:read("*all")
    f:close()
    return data
  else
    return ""
  end
end

processed = function(url)
  if downloaded[url] or addedtolist[url] then
    return true
  end
  return false
end

discover_item = function(target, item)
  if not target[item] then
--print("discovered", item)
    target[item] = true
    return true
  end
  return false
end

find_item = function(url)
  if ids[url] then
    return nil
  end
  local value = nil
  local type_ = nil
  for pattern, name in pairs({
    ["^https://[^/]*posts%.cv/([0-9a-zA-Z]+)$"]="user",
    ["^https://[^/]*posts%.cv/([0-9a-zA-Z]+/[0-9a-zA-Z]+)$"]="post",
  }) do
    value = string.match(url, pattern)
    type_ = name
    if value then
      break
    end
  end
  if value and type_ then
    return {
      ["value"]=value,
      ["type"]=type_
    }
  end
end

set_item = function(url)
  found = find_item(url)
  if found then
    local newcontext = {}
    new_item_type = found["type"]
    new_item_value = found["value"]
    if new_item_type == "post" then
      new_item_value = string.gsub(new_item_value, "/", ":")
    end
    new_item_name = new_item_type .. ":" .. new_item_value
    local username, post_id = string.match(new_item_value, "([^:]+):([^:]+)$")
    newcontext["username"] = username
    newcontext["post_id"] = post_id
    newcontext["keys"] = {["DATAdtAAZAA0"]=true}
    if new_item_name ~= item_name
      and not ids[post_id] then
      ids = {}
      context = newcontext
      item_value = new_item_value
      item_type = new_item_type
      ids[string.lower(item_value)] = true
      if post_id then
        ids[string.lower(post_id)] = true
      end
      abortgrab = false
      tries = 0
      retry_url = false
      is_initial_url = true
      item_name = new_item_name
      print("Archiving item " .. item_name)
    end
  end
end

percent_encode_url = function(url)
  temp = ""
  for c in string.gmatch(url, "(.)") do
    local b = string.byte(c)
    if b < 32 or b > 126 then
      c = string.format("%%%02X", b)
    end
    temp = temp .. c
  end
  return temp
end

allowed = function(url, parenturl)
  local noscheme = string.match(url, "^https?://(.*)$")

  if ids[url]
    or string.match(url, "^https?://maitake%-project%.uc%.r%.appspot%.com/graphql")
    or (noscheme and ids[string.lower(noscheme)]) then
    return true
  end

  if string.match(url, "^https?://res%.cloudinary%.com/.")
    or string.match(url, "^https?://firebasestorage%.googleapis%.com/.") then
    if not context["allow_assets"] then
      return false
    else
      ids[url] = true
      return true
    end
  end

  local skip = false
  for pattern, type_ in pairs({
    ["^https://[^/]*posts%.cv/([0-9a-zA-Z_]+)$"]="user",
    ["^https://[^/]*posts%.cv/([0-9a-zA-Z]+/[0-9a-zA-Z]+)$"]="post"
  }) do
    match = string.match(url, pattern)
    if match then
      if type_ ~= "asset" then
        match = string.gsub(match, "/", ":")
      end
      local new_item = type_ .. ":" .. match
      if new_item ~= item_name
        and not ids[string.lower(string.match(match, "([^:]+)$"))] then
        discover_item(discovered_items, new_item)
        skip = true
      end
    end
  end
  if skip then
    return false
  end

  if not string.match(url, "^https?://[^/]*posts%.cv/")
    and not string.match(url, "^https?://[^/]*read%.cv/") then
    discover_item(discovered_outlinks, string.match(percent_encode_url(url), "^([^%s]+)"))
    return false
  end

  for _, pattern in pairs({
    "([0-9a-zA-Z_]+)"
  }) do
    for s in string.gmatch(url, pattern) do
      if ids[string.lower(s)] then
        return true
      end
    end
  end

  return false
end

wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  local url = urlpos["url"]["url"]
  local html = urlpos["link_expect_html"]

  --[[if allowed(url, parent["url"])
    and not processed(url)
    and string.match(url, "^https://")
    and not addedtolist[url] then
    addedtolist[url] = true
    return true
  end]]

  return false
end

decode_codepoint = function(newurl)
  newurl = string.gsub(
    newurl, "\\[uU]([0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F])",
    function (s)
      return utf8.char(tonumber(s, 16))
    end
  )
  return newurl
end

percent_encode_url = function(newurl)
  result = string.gsub(
    newurl, "(.)",
    function (s)
      local b = string.byte(s)
      if b < 32 or b > 126 then
        return string.format("%%%02X", b)
      end
      return s
    end
  )
  return result
end

wget.callbacks.get_urls = function(file, url, is_css, iri)
  local urls = {}
  local html = nil
  local json = nil
  local post_data = nil

  downloaded[url] = true

  if abortgrab then
    return {}
  end

  local function fix_case(newurl)
    if not newurl then
      newurl = ""
    end
    if not string.match(newurl, "^https?://[^/]") then
      return newurl
    end
    if string.match(newurl, "^https?://[^/]+$") then
      newurl = newurl .. "/"
    end
    local a, b = string.match(newurl, "^(https?://[^/]+/)(.*)$")
    return string.lower(a) .. b
  end

  local function check(newurl)
    if not string.match(newurl, "^https?://") then
      return nil
    end
    local post_body = nil
    local post_url = nil
    if not newurl then
      newurl = ""
    end
    newurl = decode_codepoint(newurl)
    newurl = fix_case(newurl)
    local origurl = url
    if string.len(url) == 0
      or string.len(newurl) == 0 then
      return nil
    end
    local url = string.match(newurl, "^([^#]+)")
    local url_ = string.match(url, "^(.-)[%.\\]*$")
    while string.find(url_, "&amp;") do
      url_ = string.gsub(url_, "&amp;", "&")
    end
    if not processed(url_ .. tostring(post_data))
      and allowed(url_, origurl) then
      local headers = {}
      if string.match(url_, "^https?://[^/]+/_next/data/") then
        headers["x-nextjs-data"] = "1"
      end
      if post_data then
        if string.match(url_, "graphql") then
          headers["Content-Type"] = "application/json"
        end
        table.insert(urls, {
          url=url_,
          headers=headers,
          body_data=post_data,
          method="POST"
        })
      else
        table.insert(urls, {
          url=url_,
          headers=headers
        })
      end
      addedtolist[url_ .. tostring(post_data)] = true
      addedtolist[url] = true
    end
  end

  local function checknewurl(newurl)
    if not newurl then
      newurl = ""
    end
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "['\"><]") then
      return nil
    end
    if string.match(newurl, "^https?:////") then
      check(string.gsub(newurl, ":////", "://"))
    elseif string.match(newurl, "^https?://") then
      check(newurl)
    elseif string.match(newurl, "^https?:\\/\\?/") then
      check(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^\\/\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^//") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^/") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^%.%./") then
      if string.match(url, "^https?://[^/]+/[^/]+/") then
        check(urlparse.absolute(url, newurl))
      else
        checknewurl(string.match(newurl, "^%.%.(/.+)$"))
      end
    elseif string.match(newurl, "^%./") then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function checknewshorturl(newurl)
    if not newurl then
      newurl = ""
    end
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "^%?") then
      check(urlparse.absolute(url, newurl))
    elseif not (
      string.match(newurl, "^https?:\\?/\\?//?/?")
      or string.match(newurl, "^[/\\]")
      or string.match(newurl, "^%./")
      or string.match(newurl, "^[jJ]ava[sS]cript:")
      or string.match(newurl, "^[mM]ail[tT]o:")
      or string.match(newurl, "^vine:")
      or string.match(newurl, "^android%-app:")
      or string.match(newurl, "^ios%-app:")
      or string.match(newurl, "^data:")
      or string.match(newurl, "^irc:")
      or string.match(newurl, "^%${")
    ) then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function set_new_params(newurl, data)
    for param, value in pairs(data) do
      if value == nil then
        value = ""
      elseif type(value) == "string" then
        value = "=" .. value
      end
      if string.match(newurl, "[%?&]" .. param .. "[=&]") then
        newurl = string.gsub(newurl, "([%?&]" .. param .. ")=?[^%?&;]*", "%1" .. value)
      else
        if string.match(newurl, "%?") then
          newurl = newurl .. "&"
        else
          newurl = newurl .. "?"
        end
        newurl = newurl .. param .. value
      end
    end
    return newurl
  end

  local function increment_param(newurl, param, default, step)
    local value = string.match(newurl, "[%?&]" .. param .. "=([0-9]+)")
    if value then
      value = tonumber(value)
      value = value + step
      return set_new_params(newurl, {[param]=tostring(value)})
    else
      if default ~= nil then
        default = tostring(default)
      end
      return set_new_params(newurl, {[param]=default})
    end
  end

  local function get_count(data)
    local count = 0
    for _ in pairs(data) do
      count = count + 1
    end 
    return count
  end

  local function queue_graphql(newurl, json)
    if post_data then
      error("Unexpectedly found POST data.")
    end
    post_data = cjson.encode(json)
    check(newurl)
    local query_name = string.match(json["query"], "query ([^%(]+)%(")
    newurl = newurl .. "?query_name=" .. query_name
    if json["variables"] then
      for k, v in pairs(json["variables"]) do
        newurl = newurl .. "&variables_" .. tostring(k) .. "=" .. tostring(v)
      end
      check(newurl)
    end
    post_data = nil
  end

  local function queue_asset(newurl)
    context["allow_assets"] = true
    check(newurl)
    context["allow_assets"] = false
  end

  local function create_asset(s, s_type, c_default, h_num, w_num, h, w)
    local a, b = string.match(s, "^(https://firebasestorage%.googleapis%.com/v0/b/[^/%.]+%.appspot%.com/o/)([^%?]+)")
    local folder = ({
      ["https://firebasestorage.googleapis.com/v0/b/maitake-project.appspot.com/o/"]=1,
      ["https://firebasestorage.googleapis.com/v0/b/cv-development.appspot.com/o/"]=2
    })[a]
    local args = nil
    if s_type == "image" then
      if h_num > 0 and w_num == -1 then
        w_num = math.floor(h_num/h*w)
      elseif w_num > 0 and h_num == -1 then
        h_num = math.floor(w_num/w*h)
      end
      args = "c_" .. c_default .. ",h_" .. h_num .. ",w_" .. w_num .. "/dpr_1.0"
    elseif s_type == "video" then
      args = "t_v_a"
    else
      error("Unexpected media item type " .. s_type .. ".")
    end
    queue_asset("https://res.cloudinary.com/read-cv/" .. s_type .. "/upload/" .. args .. "/v1/" .. tostring(folder) .. "/" .. urlparse.unescape(b) .. "?_a=DATAdtAAZAA0")
  end

  local function discover_users(json)
    for k, v in pairs(json) do
      if type(v) == "table" then
        discover_users(v)
      elseif k == "username" and type(v) == "string" then
        check("https://posts.cv/" .. v)
      end
    end
  end

  local function extract_post(json)
    local username = json["poster"]["username"]
    if not username or username == cjson.null then
      return nil
    end
    local id = json["id"]
    if not string.match(id, "^posts/") then
      error("Found unexpected post " .. id .. ".")
    end
    check("https://posts.cv/" .. username .. "/" .. string.match(id, "/([^/]+)$"))
    if json["inReplyTo"] and json["inReplyTo"] ~= cjson.null then
      extract_post(json["inReplyTo"])
    end
    if json["repostedPost"] and json["repostedPost"] ~= cjson.null then
      extract_post(json["repostedPost"])
    end
  end

  if string.match(url, "^https://res%.cloudinary%.com/read%-cv/image/upload/c_[^,]+,h_[0-9]+,w_[0-9]+/") then
    local a, b = string.match(url, "^(.-)/c_[^,]+,h_[0-9]+,w_[0-9]+/?d?p?r?_?[0-9%.]*(.+)$")
    if not string.match(b, "^/") then
      b = "/" .. b
    end
    for _, dpr in pairs({"", "/dpr_1.0"}) do
      for _, res in pairs({
        "",
        "/c_limit,h_2048,w_2048",
        "/c_fill,h_28,w_28",
        "/c_fill,h_48,w_48",
        "/c_fill,h_92,w_92",
        "/c_fill,h_90,w_90",
        "/c_limit,h_430,w_430",
      }) do
        queue_asset(a .. res .. dpr .. b)
      end
    end
  end

  if string.match(url, "^https?://res%.cloudinary%.com/read%-cv/video/upload/t_v_[a-z]/") then
    local a, b = string.match(url, "^(.-)/t_v_[a-z](.+)$")
    for _, res in pairs({
      "",
      "/t_v_a",
      "/t_v_p",
      "/t_v_h"
    }) do
      queue_asset(a .. res .. b)
    end
  end

  if string.match(url, "%?_a=") then
    local base = string.match(url, "^([^%?]+)")
    queue_asset(base)
    for key, _ in pairs(context["keys"]) do
      queue_asset(base .. "?_a=" .. key)
    end
  end

  if allowed(url)
    and status_code < 300
    and item_type ~= "asset" then
    html = read_file(file)
    if string.match(url, "/graphql$") then
      json = cjson.decode(html)["data"]
      discover_users(json)
    end
    if item_type == "post" then
      if string.match(url, "/graphql$") then
        if not json["post"]["embeds"]
          and not json["post"]["likers"]
          and not json["post"]["reposters"] then
          error("Could not find data in post JSON.")
        end
        if json["post"]["embeds"] and json["post"]["embeds"] ~= cjson.null then
          for _, data in pairs(json["post"]["embeds"]) do
            if data["type"] == "gallery" then
              for _, data2 in pairs(data["payload"]["media"]) do
                --check(data2["url"])
                create_asset(data2["url"], data2["type"], "limit", -1, 430, data2["height"], data2["width"])
              end
            else
              error("Unexpected media type " .. data["type"] .. ".")
            end
          end
        end
      else
        for k in string.gmatch(html, "%?_a=([0-9a-zA-Z]+)") do
          context["keys"][k] = true
        end
      end
      queue_graphql(
        "https://maitake-project.uc.r.appspot.com/graphql",
        {
          ["query"]="query PermalinkQuery(\n  $id: String!\n) {\n  post(id: $id) {\n    id\n    content\n    canSee\n    embeds {\n      type\n      payload\n    }\n    timestamp\n    poster {\n      id\n      displayName\n      username\n      photoURL\n      supporterInfo {\n        tier\n      }\n    }\n    likeCount\n    repostCount\n    mentions {\n      username\n      id\n    }\n    inReplyTo {\n      id\n      poster {\n        id\n        username\n        displayName\n      }\n      mentions {\n        id\n        username\n        displayName\n      }\n    }\n    thread {\n      id\n      deleted\n      ...PermalinkThreadingInfo_post\n      ...Post_post\n    }\n    deleted\n    highlighted\n    canReply\n    isMuted\n    replyPrivacy\n    isBookmarked\n    ...PostContent_post\n    ...PostAttachment_post\n    ...PermalinkUFIButtonsLikeButton_post\n    ...PermalinkUFIButtonsRepostButton_post\n  }\n}\n\nfragment PermalinkThreadingInfo_post on Post {\n  id\n  inReplyTo {\n    id\n  }\n  poster {\n    id\n    username\n  }\n  deleted\n  timestamp\n  replyPrivacy\n}\n\nfragment PermalinkUFIButtonsLikeButton_post on Post {\n  id\n  isLiked\n}\n\nfragment PermalinkUFIButtonsRepostButton_post on Post {\n  id\n  isReposted\n}\n\nfragment PostAttachment_post on Post {\n  embeds {\n    type\n    payload\n  }\n}\n\nfragment PostContent_post on Post {\n  content\n  links {\n    type\n    payload\n    start\n    length\n    attachment {\n      __typename\n      ... on PostLinkUserAttachment {\n        user {\n          id\n          username\n        }\n      }\n    }\n  }\n}\n\nfragment PostLikeButton_post on Post {\n  id\n  likeCount\n  isLiked\n}\n\nfragment PostReplyingTo_post on Post {\n  inReplyTo {\n    poster {\n      username\n      id\n    }\n    mentions {\n      username\n      id\n    }\n    id\n  }\n  poster {\n    username\n    id\n  }\n}\n\nfragment PostRepostButton_post on Post {\n  id\n  repostCount\n  isReposted\n}\n\nfragment Post_post on Post {\n  id\n  poster {\n    id\n    displayName\n    username\n    photoURL\n    supporterInfo {\n      tier\n    }\n  }\n  replyCount\n  repostCount\n  content\n  timestamp\n  canSee\n  isMuted\n  deleted\n  highlighted\n  canReply\n  isBookmarked\n  ...PostReplyingTo_post\n  ...PostContent_post\n  ...PostAttachment_post\n  ...PostLikeButton_post\n  ...PostRepostButton_post\n}\n",
          ["variables"]={
            ["id"]=context["post_id"]
          }
        }
      )
      queue_graphql(
        "https://maitake-project.uc.r.appspot.com/graphql",
        {
          ["query"]="query PermalinkLikesQuery(\n  $id: String!\n) {\n  post(id: $id) {\n    id\n    likers {\n      edges {\n        node {\n          id\n          displayName\n          username\n          photoURL\n        }\n      }\n    }\n  }\n}\n",
          ["variables"]={
            ["id"]=context["post_id"]
          }
        }
      )
      queue_graphql(
        "https://maitake-project.uc.r.appspot.com/graphql",
        {
          ["query"]="query PermalinkRepostsQuery(\n  $id: String!\n) {\n  post(id: $id) {\n    id\n    reposters {\n      edges {\n        node {\n          id\n          displayName\n          username\n          photoURL\n        }\n      }\n    }\n  }\n}\n",
          ["variables"]={
            ["id"]=context["post_id"]
          }
        }
      )
      check("https://posts.cv/_next/data/uK9ax5GMJ6K7xhSB1bl2S/" .. context["username"] .. "/" .. item_value .. ".json?username=" .. context["username"] .. "&projectSlug=" .. item_value)
    elseif item_type == "user" then
      check("https://posts.cv/_next/data/uK9ax5GMJ6K7xhSB1bl2S/" .. item_value .. ".json?username=" .. item_value)
      if string.match(url, "/graphql$")
        and json["userByUsername"] then
        local photo_url = json["userByUsername"]["photoURL"]
        if photo_url then
          create_asset(photo_url, "image", "fill", 92, 92, -1, -1)
        end
        local all_profile_items = json["userByUsername"]["allProfileItems"]
        local feed = json["userByUsername"]["postsFeed"]
        if all_profile_items then
          for _, data in pairs(all_profile_items) do
            for _, data2 in pairs({
              data["content"]["images"],
              data["content"]["attachments"],
              data["attachments"]
            }) do
              if data2 then
                for _, d in pairs(data2) do
                  if not d["src"] and d["payload"] and d["payload"] ~= cjson.null then
                    d = d["payload"]
                  end
                  if not d["src"] and d["data"] and d["data"] ~= cjson.null then
                    d = d["data"]
                  end
                  if d["type"] or d["src"] then
                    local image_type = string.match(d["type"], "^([^/]+)")
                    create_asset(d["src"], image_type, "fill", 90, -1, d["height"], d["width"])
                  end
                end
              end
            end
          end
        end
        if feed then
          for _, data in pairs(feed["edges"]) do
            extract_post(data["node"])
          end
          local end_cursor = feed["pageInfo"]["endCursor"]
          if end_cursor and end_cursor ~= cjson.null then
            for _, feed_type in pairs({"all", "media", "topLevel"}) do
              queue_graphql(
                "https://maitake-project.uc.r.appspot.com/graphql",
                {
                  ["query"]="query ProfileLoadMoreQuery(\n  $username: String!\n  $cursor: String!\n  $feedType: String!\n) {\n  userByUsername(username: $username) {\n    id\n    postsFeed(feedType: $feedType, first: 50, after: $cursor) {\n      edges {\n        node {\n          id\n          ...ProfileTabsContent_post\n        }\n      }\n      pageInfo {\n        hasNextPage\n        endCursor\n      }\n    }\n  }\n}\n\nfragment PostAttachment_post on Post {\n  embeds {\n    type\n    payload\n  }\n}\n\nfragment PostContent_post on Post {\n  content\n  links {\n    type\n    payload\n    start\n    length\n    attachment {\n      __typename\n      ... on PostLinkUserAttachment {\n        user {\n          id\n          username\n        }\n      }\n    }\n  }\n}\n\nfragment PostLikeButton_post on Post {\n  id\n  likeCount\n  isLiked\n}\n\nfragment PostReplyingTo_post on Post {\n  inReplyTo {\n    poster {\n      username\n      id\n    }\n    mentions {\n      username\n      id\n    }\n    id\n  }\n  poster {\n    username\n    id\n  }\n}\n\nfragment PostRepostButton_post on Post {\n  id\n  repostCount\n  isReposted\n}\n\nfragment Post_post on Post {\n  id\n  poster {\n    id\n    displayName\n    username\n    photoURL\n    supporterInfo {\n      tier\n    }\n  }\n  replyCount\n  repostCount\n  content\n  timestamp\n  canSee\n  isMuted\n  deleted\n  highlighted\n  canReply\n  isBookmarked\n  ...PostReplyingTo_post\n  ...PostContent_post\n  ...PostAttachment_post\n  ...PostLikeButton_post\n  ...PostRepostButton_post\n}\n\nfragment ProfileTabsContent_post on Post {\n  id\n  embedType\n  embedPayload\n  ...Post_post\n  poster {\n    displayName\n    username\n    id\n  }\n  repostedPost {\n    id\n    ...Post_post\n  }\n}\n",
                  ["variables"]=
                  {
                    ["username"]=item_value,
                    ["cursor"]=end_cursor,
                    ["feedType"]=feed_type
                  }
                }
              )
            end
          end
        end
      end
      for _, feed_type in pairs({"all", "media", "topLevel"}) do
        queue_graphql(
          "https://maitake-project.uc.r.appspot.com/graphql",
          {
            ["query"]="query ProfileQuery(\n  $username: String!\n  $feedType: String!\n) {\n  userByUsername(username: $username) {\n    id\n    ...ProfileContent_user\n    postsFeed(feedType: $feedType, first: 50) {\n      edges {\n        node {\n          id\n          ...ProfileTabsContent_post\n        }\n      }\n      pageInfo {\n        hasNextPage\n        endCursor\n      }\n    }\n  }\n}\n\nfragment PostAttachment_post on Post {\n  embeds {\n    type\n    payload\n  }\n}\n\nfragment PostContent_post on Post {\n  content\n  links {\n    type\n    payload\n    start\n    length\n    attachment {\n      __typename\n      ... on PostLinkUserAttachment {\n        user {\n          id\n          username\n        }\n      }\n    }\n  }\n}\n\nfragment PostLikeButton_post on Post {\n  id\n  likeCount\n  isLiked\n}\n\nfragment PostReplyingTo_post on Post {\n  inReplyTo {\n    poster {\n      username\n      id\n    }\n    mentions {\n      username\n      id\n    }\n    id\n  }\n  poster {\n    username\n    id\n  }\n}\n\nfragment PostRepostButton_post on Post {\n  id\n  repostCount\n  isReposted\n}\n\nfragment Post_post on Post {\n  id\n  poster {\n    id\n    displayName\n    username\n    photoURL\n    supporterInfo {\n      tier\n    }\n  }\n  replyCount\n  repostCount\n  content\n  timestamp\n  canSee\n  isMuted\n  deleted\n  highlighted\n  canReply\n  isBookmarked\n  ...PostReplyingTo_post\n  ...PostContent_post\n  ...PostAttachment_post\n  ...PostLikeButton_post\n  ...PostRepostButton_post\n}\n\nfragment ProfileContent_user on User {\n  id\n  displayName\n  canSee\n  title\n  location\n  pronouns\n  website\n  photoURL\n  username\n  replyPrivacy\n  supporterInfo {\n    tier\n  }\n  verified\n}\n\nfragment ProfileTabsContent_post on Post {\n  id\n  embedType\n  embedPayload\n  ...Post_post\n  poster {\n    displayName\n    username\n    id\n  }\n  repostedPost {\n    id\n    ...Post_post\n  }\n}\n",
            ["variables"]={
              ["username"]=item_value,
              ["feedType"]=feed_type
            }
          }
        )
      end
      check("https://read.cv/_next/data/uK9ax5GMJ6K7xhSB1bl2S/" .. item_value .. ".json?username=" .. item_value)
      check("https://read.cv/" .. item_value)
      queue_graphql(
        "https://maitake-project.uc.r.appspot.com/graphql",
        {
          ["query"]="query ProfileGraphQLQuery(\n  $username: String!\n) {\n  userByUsername(username: $username) {\n    id\n    uid\n    allProfileItems {\n      id\n      collection\n      content\n      ...ProfileItem_profileItem\n    }\n    allContactItems {\n      id\n      contactType\n      contactValue\n      channelName\n      channelURL\n      order\n    }\n    currentTeams {\n      id\n      username\n      profilePhotoURL\n      teamName\n    }\n    pastTeams {\n      id\n      username\n      profilePhotoURL\n      teamName\n    }\n    statusEmoji {\n      description\n      emoji\n    }\n    statusBody\n    statusTimestamp\n    about\n    photoURL\n    displayName\n    username\n    replyPrivacy\n    latestStatus {\n      id\n    }\n    supporterInfo {\n      tier\n    }\n    verified\n    website\n    title\n    location\n    pronouns\n    openGraphImageURL\n    sectionOrder\n    printAbout\n    printAwards\n    printContact\n    printEducation\n    printExhibitions\n    printFeatures\n    printProjects\n    printSideProjects\n    printSpeaking\n    printWorkExperience\n    printVolunteering\n    printCertifications\n    printWriting\n    printTeams\n    writingTab {\n      totalCount\n    }\n    ...ProfileWritingTabContent_user\n    ...ProfileMessageButtonFragment_user\n  }\n}\n\nfragment ProfileItem_profileItem on ProfileItem {\n  attachments {\n    __typename\n    ...ScrollableProfileItemGallery_profileItemAttachment\n  }\n}\n\nfragment ProfileMessageButtonFragment_user on User {\n  canSee\n  replyPrivacy\n  uid\n  username\n}\n\nfragment ProfileWritingTabContentPageRow_writingTabNode on WritingTabNode {\n  page {\n    id\n    slug\n    publishedContent {\n      title\n      content\n      thumbnail\n    }\n    isPrivate\n  }\n  publishedAt\n}\n\nfragment ProfileWritingTabContent_user on User {\n  id\n  username\n  writingTab {\n    edges {\n      node {\n        publishedAt\n        ...ProfileWritingTabContentPageRow_writingTabNode\n      }\n    }\n  }\n}\n\nfragment ScrollableProfileItemGallery_profileItemAttachment on ProfileItemAttachment {\n  __isProfileItemAttachment: __typename\n  __typename\n  ... on ProfileItemMediaAttachment {\n    data\n  }\n  ... on ProfileItemCaseStudyAttachment {\n    caseStudy {\n      id\n      pageStatus\n      slug\n    }\n  }\n}\n",
          ["variables"]={
            ["username"]=item_value
          }
        }
      )
    end
    for newurl in string.gmatch(string.gsub(html, "&[qQ][uU][oO][tT];", '"'), '([^"]+)') do
      if json then
        check(newurl)
      else
        checknewurl(newurl)
      end
    end
    for newurl in string.gmatch(string.gsub(html, "&#039;", "'"), "([^']+)") do
      if json then
        check(newurl)
      else
        checknewurl(newurl)
      end
    end
    for newurl in string.gmatch(html, "[^%-]href='([^']+)'") do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, '[^%-]href="([^"]+)"') do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, ":%s*url%(([^%)]+)%)") do
      checknewurl(newurl)
    end
    html = string.gsub(html, "&gt;", ">")
    html = string.gsub(html, "&lt;", "<")
    for newurl in string.gmatch(html, ">%s*([^<%s]+)") do
      checknewurl(newurl)
    end
  end

  return urls
end

wget.callbacks.write_to_warc = function(url, http_stat)
  status_code = http_stat["statcode"]
  set_item(url["url"])
  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
  io.stdout:flush()
  logged_response = true
  if not item_name then
    error("No item name found.")
  end
  is_initial_url = false
  if http_stat["statcode"] ~= 200
    and http_stat["statcode"] ~= 404 then
    retry_url = true
    return false
  end
  if http_stat["len"] == 0
    and http_stat["statcode"] < 300 then
    retry_url = true
    return false
  end
  if abortgrab then
    print("Not writing to WARC.")
    return false
  end
  retry_url = false
  tries = 0
  return true
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  status_code = http_stat["statcode"]

  if not logged_response then
    url_count = url_count + 1
    io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
    io.stdout:flush()
  end
  logged_response = false

  if killgrab then
    return wget.actions.ABORT
  end

  set_item(url["url"])
  if not item_name then
    error("No item name found.")
  end

  if abortgrab then
    abort_item()
    return wget.actions.EXIT
  end

  if status_code == 0 or retry_url then
    io.stdout:write("Server returned bad response. ")
    io.stdout:flush()
    tries = tries + 1
    local maxtries = 11
    if tries > maxtries then
      io.stdout:write(" Skipping.\n")
      io.stdout:flush()
      tries = 0
      abort_item()
      return wget.actions.EXIT
    end
    local sleep_time = math.random(
      math.floor(math.pow(2, tries-0.5)),
      math.floor(math.pow(2, tries))
    )
    io.stdout:write("Sleeping " .. sleep_time .. " seconds.\n")
    io.stdout:flush()
    os.execute("sleep " .. sleep_time)
    return wget.actions.CONTINUE
  else
    if status_code == 200 then
      if not seen_200[url["url"]] then
        seen_200[url["url"]] = 0
      end
      seen_200[url["url"]] = seen_200[url["url"]] + 1
    end
    downloaded[url["url"]] = true
  end

  if status_code >= 300 and status_code <= 399 then
    local newloc = urlparse.absolute(url["url"], http_stat["newloc"])
    if processed(newloc) or not allowed(newloc, url["url"]) then
      tries = 0
      return wget.actions.EXIT
    end
  end

  tries = 0

  return wget.actions.NOTHING
end

wget.callbacks.finish = function(start_time, end_time, wall_time, numurls, total_downloaded_bytes, total_download_time)
  local function submit_backfeed(items, key)
    local tries = 0
    local maxtries = 5
    while tries < maxtries do
      if killgrab then
        return false
      end
      local body, code, headers, status = http.request(
        "https://legacy-api.arpa.li/backfeed/legacy/" .. key,
        items .. "\0"
      )
      if code == 200 and body ~= nil and cjson.decode(body)["status_code"] == 200 then
        io.stdout:write(string.match(body, "^(.-)%s*$") .. "\n")
        io.stdout:flush()
        return nil
      end
      io.stdout:write("Failed to submit discovered URLs." .. tostring(code) .. tostring(body) .. "\n")
      io.stdout:flush()
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
    end
    kill_grab()
    error()
  end

  local file = io.open(item_dir .. "/" .. warc_file_base .. "_bad-items.txt", "w")
  for url, _ in pairs(bad_items) do
    file:write(url .. "\n")
  end
  file:close()
  for key, data in pairs({
    ["postscv-f934ffkug070sc45"] = discovered_items,
    ["urls-n7ydiob4e7jy3cj2"] = discovered_outlinks
  }) do
    print("queuing for", string.match(key, "^(.+)%-"))
    local items = nil
    local count = 0
    for item, _ in pairs(data) do
      print("found item", item)
      if items == nil then
        items = item
      else
        items = items .. "\0" .. item
      end
      count = count + 1
      if count == 1000 then
        submit_backfeed(items, key)
        items = nil
        count = 0
      end
    end
    if items ~= nil then
      submit_backfeed(items, key)
    end
  end
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if killgrab then
    return wget.exits.IO_FAIL
  end
  if abortgrab then
    abort_item()
  end
  return exit_status
end


