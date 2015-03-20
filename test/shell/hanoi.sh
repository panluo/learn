#/bin/bash
E_NOPARAM=66    #没有参数传给脚本
E_BADPARAM=67   #传给脚本的盘子个数不符合要求.
Moves=0          #保存移动次数的全局变量.

dohanoi(){
    case $1 in
        0)
            ;;
        *)
            dohanoi "$(($1-1))" $2 $4 $3
            echo move $2 "-->" $3
            Moves=$(($Moves+1))
          #  set "Moves +=1"
            dohanoi "$(($1-1))" $4 $3 $2
            ;;
        esac
}
case $# in
    1)
        case $(($1>0)) in   #至少还有一个盘子
        1)
            dohanoi $1 1 3 2
            echo "Total moves = $Moves"
            exit 0;
            ;;
        *)
            echo "$0:illegal value for number of disks";
            exit $E_BADPARAM;
            ;;
        esac
        ;;
    *)
        echo "usage : $o N"
        echo "  where \"N\" is the number of disks."
        exit $E_NOPARAM;
        ;;
esac
