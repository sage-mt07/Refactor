using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace KsqlDsl.Core.Modeling
{
    public class AvroPropertyBuilder<T, TProperty> where T : class
    {
        private readonly AvroEntityTypeBuilder<T> _entityBuilder;
        private readonly PropertyInfo _property;

        internal AvroPropertyBuilder(AvroEntityTypeBuilder<T> entityBuilder, PropertyInfo property)
        {
            _entityBuilder = entityBuilder ?? throw new ArgumentNullException(nameof(entityBuilder));
            _property = property ?? throw new ArgumentNullException(nameof(property));
        }

        /// <summary>
        /// プロパティを必須に設定
        /// </summary>
        public AvroPropertyBuilder<T, TProperty> IsRequired(bool required = true)
        {
            // Avro側での必須設定は将来的に実装
            return this;
        }

        /// <summary>
        /// 最大長を設定
        /// </summary>
        public AvroPropertyBuilder<T, TProperty> HasMaxLength(int maxLength)
        {
            if (maxLength <= 0)
                throw new ArgumentException("Max length must be greater than 0", nameof(maxLength));

            // Avro側での長さ制約は将来的に実装
            return this;
        }

        /// <summary>
        /// 精度とスケールを設定（Decimal用）
        /// </summary>
        public AvroPropertyBuilder<T, TProperty> HasPrecision(int precision, int scale)
        {
            if (precision <= 0)
                throw new ArgumentException("Precision must be greater than 0", nameof(precision));
            if (scale < 0 || scale > precision)
                throw new ArgumentException("Scale must be between 0 and precision", nameof(scale));

            // DecimalPrecisionAttributeの動的設定は複雑なため、将来的に実装
            return this;
        }

        /// <summary>
        /// デフォルト値を設定
        /// </summary>
        public AvroPropertyBuilder<T, TProperty> HasDefaultValue(TProperty defaultValue)
        {
            // Avro側でのデフォルト値設定は将来的に実装
            return this;
        }

        /// <summary>
        /// プロパティを無視対象に設定
        /// </summary>
        public AvroPropertyBuilder<T, TProperty> IsIgnored(bool ignored = true)
        {
            // KafkaIgnoreAttributeの動的設定は複雑なため、将来的に実装
            return this;
        }

        /// <summary>
        /// エンティティビルダーに戻る
        /// </summary>
        public AvroEntityTypeBuilder<T> Entity()
        {
            return _entityBuilder;
        }

        /// <summary>
        /// 暗黙的なエンティティビルダー変換
        /// </summary>
        public static implicit operator AvroEntityTypeBuilder<T>(AvroPropertyBuilder<T, TProperty> propertyBuilder)
        {
            return propertyBuilder._entityBuilder;
        }
    }

}
