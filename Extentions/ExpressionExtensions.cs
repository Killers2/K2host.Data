using System;
using System.Collections.ObjectModel;
using System.Linq;
using System.Linq.Expressions;

namespace K2host.Data.Extentions
{

    /// <summary>
    /// 
    /// </summary>
    public static class ExpressionExtensions
    {

        /// <summary>
        /// Given an expression for a method that takes in a single parameter (and returns a bool), this method converts the parameter type of the parameter
        /// from TSource to TTarget. https://haacked.com/archive/2019/07/29/query-filter-by-interface/
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <typeparam name="TTarget"></typeparam>
        /// <param name="root"></param>
        /// <returns></returns>
        public static Expression<Func<TTarget, bool>> Convert<TSource, TTarget>(this Expression<Func<TSource, bool>> root)
        {
            var visitor = new ParameterTypeVisitor<TSource, TTarget>();
            return (Expression<Func<TTarget, bool>>)visitor.Visit(root);
        }
        
        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <typeparam name="TTarget"></typeparam>
        class ParameterTypeVisitor<TSource, TTarget> : ExpressionVisitor
        {

            private ReadOnlyCollection<ParameterExpression> _parameters;

            protected override Expression VisitParameter(ParameterExpression node) 
                => _parameters?.FirstOrDefault(p => p.Name == node.Name) ?? (node.Type == typeof(TSource) ? Expression.Parameter(typeof(TTarget), node.Name) : node);
            
            protected override Expression VisitLambda<T>(Expression<T> node)
            {
                _parameters = VisitAndConvert(node.Parameters, "VisitLambda");
                return Expression.Lambda(Visit(node.Body), _parameters);
            }
        }

    }
}
