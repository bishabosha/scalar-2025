package substructural

import NamedTuple.AnyNamedTuple
import NamedTuple.NamedTuple
import scala.util.NotGiven
import scala.annotation.meta.field

object Sub:

  sealed trait Substructural[A <: AnyNamedTuple, B <: AnyNamedTuple]

  object Substructural:
    sealed trait Z

    sealed trait MapLeaves[A <: AnyNamedTuple, F[_]]:
      type Out <: AnyNamedTuple

    sealed trait Zoom[P <: AnyNamedTuple, A <: AnyNamedTuple]:
      type Out

    private object Proof extends Substructural[NamedTuple.Empty, NamedTuple.Empty]
      with MapLeaves[NamedTuple.Empty, [_] =>> Any]
    private object AnyProof extends Substructural.Zoom[NamedTuple.Empty, NamedTuple.Empty]

    def emptyProof: Substructural[NamedTuple.Empty, NamedTuple.Empty] = Proof
    def emptyLeavesProof: Substructural.MapLeaves[NamedTuple.Empty, [_] =>> Any] = Proof
    def emptyZoomProof: Substructural.Zoom[NamedTuple.Empty, NamedTuple.Empty] = AnyProof

    transparent inline def isNamedTuple[A <: AnyNamedTuple]: Boolean =
      ${macros.isNamedTupleImpl[A]}

    transparent inline given proven: [A <: AnyNamedTuple, B <: AnyNamedTuple] => Substructural[A, B] =
      ${macros.provenImpl[A, B]}

    transparent inline given provenLeaf: [A <: AnyNamedTuple, F[_]] => Substructural.MapLeaves[A, F] =
      ${macros.provenLeafImpl[A, F]}

    transparent inline given provenZoom: [P <: AnyNamedTuple, A <: AnyNamedTuple] => Substructural.Zoom[P, A] =
      ${macros.provenZoomImpl[P, A]}

    object macros:
      import scala.quoted.*

      object NamedTupleRefl:
        def use[Q <: Quotes & Singleton](using quotes: Q)[T](op: NamedTupleRefl { type Inner = Q } ?=> T): T =
          op(using new NamedTupleRefl(using quotes).asInstanceOf[NamedTupleRefl { type Inner = Q }])

      type NTData = Map[String, Type[?]]
      type NTOrdData = (List[String], List[Type[?]])

      final class NamedTupleRefl(using val quotes: Quotes):
        import quotes.reflect.*

        type Inner = quotes.type

        val NamedTupleType = Symbol.requiredModule("scala.NamedTuple").typeMember("NamedTuple")
        val NamedTupleConcatType = Symbol.requiredModule("scala.NamedTuple").typeMember("Concat")
        val NamedTupleTypeRef = NamedTupleType.typeRef

        object NamedTupleData:
          def unapply(tp: Type[?]): Option[Map[String, Type[?]]] = unpackNamedTuple(tp): (nmes, tps) =>
            var buf = Map.empty[String, Type[?]]
            nmes.lazyZip(tps).foreach((n, t) => buf = buf.updated(n, t))
            buf

        object NamedTupleOrdData:
          def unapply(tp: Type[?]): Option[(List[String], List[Type[?]])] = unpackNamedTuple(tp): (nmes, tps) =>
            nmes -> tps

        def unpackNamedTuple[Out](tp: Type[?])(f: (List[String], List[Type[?]]) => Out): Option[Out] =
          tp match
            case '[t] => unapply(TypeRepr.of[t].simplified).flatMap: tp =>
              tp(0).asType match
                case '[nmes] =>
                  typeToStrings[nmes].flatMap: nmes =>
                    tp(1).asType match
                      case '[tps] =>
                        typeToList[tps].map: values =>
                          f(nmes, values)
                      case _ => None

        def packNamedTuple(data: NTOrdData): Type[?] =
          val (nmes, tps) = data
          val nmes0 = listToType(nmes)(n => ConstantType(StringConstant(n)).asType)
          val tps0 = listToType(tps)(identity)
          (nmes0, tps0) match
            case ('[type nmes <: Tuple; `nmes`], '[type tps <: Tuple; `tps`]) =>
              Type.of[NamedTuple[nmes, tps]]

        def listToType[T](ts: List[T])(f: T => Type[?]): Type[?] =
          ts.foldRight(Type.of[EmptyTuple]: Type[?]): (t, acc) =>
            f(t) match
              case '[ft] => acc match
                case '[type acc <: Tuple; `acc`] => Type.of[ft *: acc]

        def typeToStrings[T: Type]: Option[List[String]] =
          val buf = List.newBuilder[String]
          def loop[U: Type]: Boolean = Type.of[U] match
            case '[n *: ns] =>
              import quotes.reflect.*
              TypeRepr.of[n] match
                case ConstantType(StringConstant(s)) =>
                  buf += s
                  loop[ns]
                case _ =>
                  false
            case '[EmptyTuple] => true
            case _ => false
          end loop
          if loop[T] then Some(buf.result()) else None

        def typeToList[T: Type]: Option[List[Type[?]]] =
          val buf = List.newBuilder[Type[?]]
          def loop[U: Type]: Boolean = Type.of[U] match
            case '[n *: ns] =>
              buf += Type.of[n]
              loop[ns]
            case '[EmptyTuple] => true
            case _ => false
          end loop
          if loop[T] then Some(buf.result()) else None

        def apply(nmes: TypeRepr, vals: TypeRepr): TypeRepr =
          AppliedType(NamedTupleTypeRef, nmes :: vals :: Nil)

        def isMatchTpe(t: TypeRepr): Boolean =
          t match
            case _: MatchType =>
              true
            case _ => false

        def unapply(t: TypeRepr): Option[(TypeRepr, TypeRepr)] =
          t match
            case AppliedType(tycon, nmes :: vals :: Nil) if tycon.typeSymbol == NamedTupleType =>
              Some((nmes, vals))
            //   unapply(app.dealias.simplified)
            case tp: TermRef =>
              unapply(tp.widenTermRefByName)
            case tp: TypeRef =>
              unapply(tp.dealiasKeepOpaques)
            case OrType(tp1, tp2) =>
              (unapply(tp1), unapply(tp2)) match
                case (Some(lhsName, lhsVal), Some(rhsName, rhsVal)) if lhsName == rhsName =>
                  Some(lhsName, OrType(lhsVal, rhsVal))
                case _ => None
            case AndType(tp1, tp2) =>
              (unapply(tp1), unapply(tp2)) match
                case (Some(lhsName, lhsVal), Some(rhsName, rhsVal)) if lhsName == rhsName =>
                  Some(lhsName, AndType(lhsVal, rhsVal))
                case (lhs, None) => lhs
                case (None, rhs) => rhs
                case _ => None
            case app @ AppliedType(tycon: TypeRef, left :: right :: Nil) if tycon.typeSymbol == NamedTupleConcatType =>
              (left.asType, right.asType) match
                case (NamedTupleOrdData(lns -> lvs), NamedTupleOrdData(rns -> rvs)) =>
                  val nms0 = listToType(lns ++ rns)(n => ConstantType(StringConstant(n)).asType)
                  val tps0 = listToType(lvs ++ rvs)(identity)
                  (nms0, tps0) match
                    case ('[type nmes <: Tuple; `nmes`], '[type tps <: Tuple; `tps`]) =>
                      Some((TypeRepr.of[nmes], TypeRepr.of[tps]))
                    case _ => None
                case _ => None
            case tpe => None//reportError(s"could not reduce ${tpe.show} to NamedTuple\nSIMPLE: ${tpe.simplified}")
      end NamedTupleRefl

      def reportError(msg: String)(using Quotes) =
        quotes.reflect.report.throwError(msg)

      def dbg(data: NTData)(using Quotes) =
        data.map({case (k, '[v]) => (k, Type.show[v])}).toString

      def ntOps(using quotes: Quotes, ntOps: NamedTupleRefl): ntOps.type =
        ntOps

      def isNamedTupleImpl[A <: AnyNamedTuple: Type](using Quotes): Expr[Boolean] = NamedTupleRefl.use:
        Type.of[A] match
          case ntOps.NamedTupleData(_) => '{ true }
          case _ => '{ false }

      def provenZoomImpl[P <: AnyNamedTuple: Type, A <: AnyNamedTuple: Type](using Quotes): Expr[Substructural.Zoom[P, A]] =
        NamedTupleRefl.use:
          def go(left: Type[?], right: Type[?], path: List[String]): Expr[Substructural.Zoom[P, A]] =
            (left, right) match
              case (ntOps.NamedTupleOrdData((n1 :: _, v1 :: _)), ntOps.NamedTupleData(data)) =>
                data.get(n1) match
                  case Some(v2) =>
                    v1 match
                      case '[Z] => v2 match
                        case '[out] => '{ emptyZoomProof.asInstanceOf[Substructural.Zoom[P, A] { type Out = out }] }
                      case _ => go(v1, v2, n1 :: path)
                  case _ =>
                    reportError(s"Cannot Substructual.Zoom[${Type.show[P]}, ${Type.show[A]}] (${path.reverse.mkString(".")}):\n missing key $n1 in ${dbg(data)}")

              case _ => reportError(s"Cannot zoom Substructual.Zoom[${Type.show[P]}, ${Type.show[A]}] (${path.reverse.mkString(".")})")
          go(Type.of[P], Type.of[A], path = Nil)

      def provenLeafImpl[A <: AnyNamedTuple: Type, F[_]: Type](using Quotes): Expr[Substructural.MapLeaves[A, F]] =
        NamedTupleRefl.use:
          mapLeaves[A, F] match
            case Some('[out]) =>
              '{ emptyLeavesProof.asInstanceOf[Substructural.MapLeaves[A, F] { type Out = out } ] }
            case _ =>
              reportError(s"Cannot map leaves Substructual.MapLeaves[${Type.show[A]}, ${Type.show[F]}]")

      def mapLeaves[A <: AnyNamedTuple: Type, F[_]: Type](using Quotes, NamedTupleRefl): Option[Type[?]] =
        def go(in: Type[?]): Type[?] =
          in match
            case ntOps.NamedTupleOrdData((ns, vs)) =>
              val newData = vs.map(go)
              ntOps.packNamedTuple(ns, newData)
            case '[t] => Type.of[F[t]]
        Some(go(Type.of[A]))

      def provenImpl[A <: AnyNamedTuple: Type, B <: AnyNamedTuple: Type](using Quotes): Expr[Substructural[A, B]] =
        NamedTupleRefl.use:
          if isSubstructural[A, B] then
            '{ emptyProof.asInstanceOf[Substructural[A, B]] }
          else
            reportError(s"Cannot prove Substructual[${Type.show[A]}, ${Type.show[B]}]")

      def isSubstructural[A <: AnyNamedTuple: Type, B <: AnyNamedTuple: Type](using Quotes, NamedTupleRefl): Boolean =
        (Type.of[A], Type.of[B]) match
          case (ntOps.NamedTupleData(d1), ntOps.NamedTupleData(d2)) => tryMerge(d1, d2)
          case _ => false

      def tryMerge(d1: NTData, d2: NTData)(using Quotes, NamedTupleRefl): Boolean =
        if d1.keySet.subsetOf(d2.keySet) then
          d1.forall:
            case (n, v1) => (v1, d2.apply(n)) match
              case (ntOps.NamedTupleData(subd1), ntOps.NamedTupleData(subd2)) =>
                tryMerge(subd1, subd2)
              case ('[t1], '[t2]) => subTypes[t1, t2]
        else
          false

      def subTypes[A: Type, B: Type](using Quotes): Boolean =
        import quotes.reflect._
        TypeRepr.of[A] <:< TypeRepr.of[B]

//   class Base

//   type SubTuple[A <: AnyNamedTuple, B <: AnyNamedTuple] <: Boolean = A match
//     case NamedTuple[ns1, vs1] => SubTupleLeft[ns1, vs1, B]

//   type SubTupleLeft[Ns1 <: Tuple, Vs1 <: Tuple, B <: AnyNamedTuple] <: Boolean =
//     B match
//       case NamedTuple[ns2, vs2] =>
//         SubTupleRight[Ns1, Vs1, ns2, vs2]

//   type SubTupleRight[
//       Ns1 <: Tuple,
//       Vs1 <: Tuple,
//       Ns2 <: Tuple,
//       Vs2 <: Tuple
//   ] <: Boolean =
//     ContainsAll[Ns1, Ns2] match
//       case true  => SubParts[Ns1, Vs1, Ns2, Vs2]
//       case false => false

//   type SubParts[
//       Ns1 <: Tuple,
//       Vs1 <: Tuple,
//       Ns2 <: Tuple,
//       Vs2 <: Tuple
//   ] <: Boolean = (Ns1, Vs1) match
//     case (n1 *: ns1, v1 *: vs1) => SubPartsRec[n1, ns1, v1, vs1, Ns2, Vs2]
//     // SubPart[n1, v1, Ns2, Vs2] match
//     //   case true  => SubParts[ns1, vs1, Ns2, Vs2]
//     //   case false => false
//     case (EmptyTuple, EmptyTuple) => true
//     case _                        => false

//   type SubPartsRec[
//       N1,
//       Ns1 <: Tuple,
//       V1,
//       Vs1 <: Tuple,
//       Ns2 <: Tuple,
//       Vs2 <: Tuple
//   ] <: Boolean =
//     SubPart[N1, V1, Ns2, Vs2] match
//       case true  => SubParts[Ns1, Vs1, Ns2, Vs2]
//       case false => false

//   type SubPart[N1, V1, Ns2 <: Tuple, Vs2 <: Tuple] <: Boolean = (Ns2, Vs2) match
//     case (N1 *: _, v2 *: _)   => true // SubCompare[V1, v2]
//     case (_ *: ns2, _ *: vs2) => SubPart[N1, V1, ns2, vs2]
//     case (_, _)               => false

//   // everything is Base or AnyNamedTuple (MT cannot distinguish otherwise)
//   // type SubCompare[X, Y] <: Boolean = NamedTuple.From[X] match
//   //   case NamedTuple.From[t] => SubCompare2[t]
//   //   case _                  => false

//   // type SubCompare2[T] <: Boolean = T match
//   //   case AnyVal => true
//   //   case _      => false

//   type SubCompareLeft[Ns1 <: Tuple, Vs1 <: Tuple, Y] <: Boolean = Y match
//     case NamedTuple[ns1, vs1] => SubTupleRight[Ns1, Vs1, ns1, vs1]
//     case _                    => false

//   type ContainsAll[X <: Tuple, Y <: Tuple] <: Boolean = X match
//     case x *: xs =>
//       Tuple.Contains[Y, x] match
//         case true  => ContainsAll[xs, Y]
//         case false => false
//     case EmptyTuple => true

//   final type IsSubTuple[Base <: AnyNamedTuple] =
//     [T <: AnyNamedTuple] =>> SubTuple[T, Base] =:= true

// import Sub._

// object Demo:

//   class Page extends Base
//   class Nav extends Base

//   def test =
//     type EmptyNt = NamedTuple.Empty
//     type SingleNt = (foo: Page)
//     type Single2Nt = (foo: Nav)
//     type PairNt = (foo: Page, bar: Nav)
//     type SingleNestedNt = (foo: (bar: Page))
//     type SingleNestedWiderNt = (foo: (bar: Page, baz: Nav))

//     type IsSubEmpty = Sub.IsSubTuple[NamedTuple.Empty]
//     type IsSubSingle = Sub.IsSubTuple[(foo: Page)]
//     type IsSubPair = Sub.IsSubTuple[(foo: Page, bar: Nav)]

//     class Base

//     locally:
//       // test empty
//       summon[IsSubEmpty[NamedTuple.Empty]]
//       summon[IsSubSingle[NamedTuple.Empty]]
//       summon[IsSubPair[NamedTuple.Empty]]

//     locally:

//       // test single
//       summon[NotGiven[IsSubEmpty[SingleNt]]]
//       val x: Sub.SubTuple[SingleNt, SingleNt] = true
//       // val x1: Sub.SubTuple[SingleNt, Single2Nt] = false
//       val y: Sub.SubTuple[SingleNt, PairNt] = true
//       val y1: Sub.SubTuple[(foo: (bar: 1)), (foo: (bar: Int))] = true
//       val y2: Sub.SubTuple[SingleNestedNt, SingleNestedWiderNt] = true
//       val z: Sub.SubTuple[PairNt, SingleNt] = false
//       // summon[IsSubPair[(foo: Int)]]

//     locally:
//       type IsNamedTuple[T] = T match
//         case NamedTuple[ns, vs] => true
//         case _                  => false

//       val x: IsNamedTuple[NamedTuple.Empty] = true
//       val y: IsNamedTuple[(foo: Int)] = true
//       val z: IsNamedTuple[Int] = false
//       val q: IsNamedTuple[String] = false
