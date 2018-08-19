struct BiTree
    value
    left
    right
end

BiTree(v::String) = BiTree(v, nothing, nothing)

BiTree(v::String, l::String) = BiTree(v, BiTree(l), nothing)

BiTree(v::String, l::String, r::String) = BiTree(v, BiTree(l), BiTree(r))

is_leaf(tree::BiTree) = tree.left == nothing && tree.right == nothing

as_posix(tree::BiTree) = join(post_order(tree), "|")


function get_height(tree::BiTree)
    l_height = tree.left == nothing ? 0 : get_height(tree.left)
    r_height = tree.rigth == nothing ? 0 : get_height(tree.right)
    max(l_height, r_height) + 1
end

function pre_order(tree::BiTree)
    global l = String[]
    function _pre_order(node)
        if typeof(node) == BiTree
            push!(l, node.value)
            _pre_order(node.left)
            _pre_order(node.right)
        end
    end
    _pre_order(tree)
    l
end

function in_order(tree::BiTree)
    global l = String[]
    function _in_order(node)
        if typeof(node) == BiTree
            _in_order(node.left)
            push!(l, node.value)
            _in_order(node.right)
        end
    end
    _in_order(tree)
    l
end

function post_order(tree::BiTree)
    global l = String[]
    function _post_order(node)
        if typeof(node) == BiTree
            _post_order(node.left)
            _post_order(node.right)
            push!(l, node.value)
        end
    end
    _post_order(tree)
    l
end

function level_order(tree::BiTree)
    l = []
    cur_level = BiTree[tree]
    while length(cur_level) > 0
        append!(l, [n.value for n in cur_level])
        next_level = []
        map(cur_level) do n
            if n.left != nothing
                push!(next_level, n.left)
            end
            if n.right != nothing
                push!(next_level, n.right)
            end
        end
        cur_level = next_level
    end
    l
end 

