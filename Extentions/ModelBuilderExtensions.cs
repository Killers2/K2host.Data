using System;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore.Query;

using K2host.Core;

namespace K2host.Data.Extentions
{
    /// <summary>
    /// 
    /// </summary>
    public static class ModelBuilderExtensions
    {
        /// <summary>
        /// 
        /// </summary>
        static readonly MethodInfo SetQueryFilterMethod = typeof(ModelBuilderExtensions).GetMethods(BindingFlags.NonPublic | BindingFlags.Static).Single(t => t.IsGenericMethod && t.Name == nameof(SetQueryFilter));
        
        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="TEntityInterface"></typeparam>
        /// <param name="builder"></param>
        /// <param name="filterExpression"></param>
        public static void SetQueryFilterOnAllEntities<TEntityInterface>(this ModelBuilder builder, Expression<Func<TEntityInterface, bool>> filterExpression)
            => builder.Model.GetEntityTypes()
                .Where(t => t.BaseType == null)
                .Select(t => t.ClrType)
                .Where(t => typeof(TEntityInterface).IsAssignableFrom(t))
                .ForEach(type => {
                    builder.SetEntityQueryFilter(type, filterExpression);
                });

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="TEntityInterface"></typeparam>
        /// <param name="builder"></param>
        public static void ForAllEntitiesOf<TEntityInterface>(this ModelBuilder builder, Action<ModelBuilder, Type> Callback)
            => builder.Model.GetEntityTypes()
                .Where(t => t.BaseType == null)
                .Select(t => t.ClrType)
                .Where(t => typeof(TEntityInterface).IsAssignableFrom(t))
                .ForEach(type => {
                    Callback.Invoke(builder, type);
                });

        /// <summary>
        /// 
        /// </summary>
        /// <param name="builder"></param>
        public static void ForAllEntities(this ModelBuilder builder, Action<ModelBuilder, Type> Callback)
            => builder.Model.GetEntityTypes()
                .Where(t => t.BaseType == null)
                .Select(t => t.ClrType)
                .ForEach(type => {
                    Callback.Invoke(builder, type);
                });

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="TEntityInterface"></typeparam>
        /// <param name="builder"></param>
        /// <param name="entityType"></param>
        /// <param name="filterExpression"></param>
        static void SetEntityQueryFilter<TEntityInterface>(this ModelBuilder builder, Type entityType, Expression<Func<TEntityInterface, bool>> filterExpression)
            => SetQueryFilterMethod
                .MakeGenericMethod(entityType, typeof(TEntityInterface))
                .Invoke(null, new object[] { builder, filterExpression });
       
        /// <summary>
       /// 
       /// </summary>
       /// <typeparam name="TEntity"></typeparam>
       /// <typeparam name="TEntityInterface"></typeparam>
       /// <param name="builder"></param>
       /// <param name="filterExpression"></param>
        static void SetQueryFilter<TEntity, TEntityInterface>(this ModelBuilder builder, Expression<Func<TEntityInterface, bool>> filterExpression) 
            where TEntityInterface : class 
            where TEntity : class, TEntityInterface
            => builder
                .Entity<TEntity>()
                .AppendQueryFilter(filterExpression.Convert<TEntityInterface, TEntity>());
        
        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entityTypeBuilder"></param>
        /// <param name="expression"></param>
        static void AppendQueryFilter<T>(this EntityTypeBuilder entityTypeBuilder, Expression<Func<T, bool>> expression) 
            where T : class
        {
            var parameterType = Expression.Parameter(entityTypeBuilder.Metadata.ClrType);

            var expressionFilter = ReplacingExpressionVisitor.Replace(expression.Parameters.Single(), parameterType, expression.Body);

            if (entityTypeBuilder.Metadata.GetQueryFilter() != null)
            {
                var currentQueryFilter = entityTypeBuilder.Metadata.GetQueryFilter();
                var currentExpressionFilter = ReplacingExpressionVisitor.Replace(currentQueryFilter.Parameters.Single(), parameterType, currentQueryFilter.Body);
                expressionFilter = Expression.AndAlso(currentExpressionFilter, expressionFilter);
            }

            var lambdaExpression = Expression.Lambda(expressionFilter, parameterType);
            entityTypeBuilder.HasQueryFilter(lambdaExpression);
        }
    }

}
