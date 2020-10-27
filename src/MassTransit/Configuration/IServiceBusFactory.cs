using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using MassTransit;
using MassTransit.SubscriptionConfigurators;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Nybus.MassTransit;

namespace Nybus.Configuration
{
    public interface IServiceBusFactory
    {
        IServiceBus CreateServiceBus(MassTransitConnectionDescriptor connectionDescriptor, IQueueStrategy queueStrategy, IReadOnlyList<Action<SubscriptionBusServiceConfigurator>> subscriptions);
    }

    public class RabbitMqServiceBusFactory : IServiceBusFactory
    {
        private readonly int _concurrencyLimit;

        public RabbitMqServiceBusFactory(int concurrencyLimit)
        {
            if (concurrencyLimit <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(concurrencyLimit));
            }
            _concurrencyLimit = concurrencyLimit;
        }

        public IServiceBus CreateServiceBus(MassTransitConnectionDescriptor connectionDescriptor, IQueueStrategy queueStrategy, IReadOnlyList<Action<SubscriptionBusServiceConfigurator>> subscriptions)
        {
            return ServiceBusFactory.New(bus =>
            {
                var receiveUri = new Uri(connectionDescriptor.Host, queueStrategy.GetQueueName());

                bus.UseRabbitMq(r =>
                {
                    r.ConfigureHost(receiveUri, h =>
                    {
                        h.SetUsername(connectionDescriptor.UserName);
                        h.SetPassword(connectionDescriptor.Password);
                    });
                });

                bus.ReceiveFrom(receiveUri);
                bus.UseJsonSerializer();

                bus.ConfigureJsonSerializer(ConfigureIsoDateTimeConverters);
                bus.ConfigureJsonDeserializer(ConfigureIsoDateTimeConverters);

                bus.SetConcurrentConsumerLimit(_concurrencyLimit);

                foreach (var subscription in subscriptions)
                    bus.Subscribe(subscription);

                bus.Validate();
            });
        }

        private static JsonSerializerSettings ConfigureIsoDateTimeConverters(JsonSerializerSettings settings)
        {
            var isoDateTimeConverters = settings.Converters.OfType<IsoDateTimeConverter>().ToArray();

            foreach (var converter in isoDateTimeConverters)
            {
                converter.Culture = CultureInfo.InvariantCulture;
            }

            return settings;
        }
    }

    public class LoopbackServiceBusFactory : IServiceBusFactory
    {
        public IServiceBus CreateServiceBus(MassTransitConnectionDescriptor connectionDescriptor, IQueueStrategy queueStrategy, IReadOnlyList<Action<SubscriptionBusServiceConfigurator>> subscriptions)
        {
            UriBuilder builder = new UriBuilder(connectionDescriptor.Host) {Scheme = "loopback"};

            return ServiceBusFactory.New(bus =>
            {
                var receiveUri = new Uri(builder.Uri, queueStrategy.GetQueueName());

                bus.ReceiveFrom(receiveUri);
                bus.UseJsonSerializer();

                bus.ConfigureJsonSerializer(ConfigureIsoDateTimeConverters);
                bus.ConfigureJsonDeserializer(ConfigureIsoDateTimeConverters);

                foreach (var subscription in subscriptions)
                    bus.Subscribe(subscription);

                bus.Validate();

            });
        }

        private static JsonSerializerSettings ConfigureIsoDateTimeConverters(JsonSerializerSettings settings)
        {
            var isoDateTimeConverters = settings.Converters.OfType<IsoDateTimeConverter>().ToArray();

            foreach (var converter in isoDateTimeConverters)
            {
                converter.Culture = CultureInfo.InvariantCulture;
            }

            return settings;
        }
    }
}